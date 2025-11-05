from __future__ import annotations
from typing import Optional, List, Set
import re

from sqlglot import exp  # type: ignore

from ..models import ObjectInfo, TableSchema, ColumnSchema, ColumnLineage


def _parse_procedure_string(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
    """Parse CREATE PROCEDURE using string-based approach (extracted)."""
    # Normalize headers (SET/GO, COLLATE, etc.)
    sql_content = self._normalize_tsql(sql_content)

    # Determine DB context from USE at the start
    try:
        db_from_use = self._extract_database_from_use_statement(sql_content)
        if db_from_use:
            self.current_database = db_from_use
    except Exception:
        pass

    procedure_name = self._extract_procedure_name(sql_content) or object_hint or "unknown_procedure"

    # Prefer DB from USE; else infer from content; fallback to default
    inferred_db = self._infer_database_for_object(statement=None, sql_text=sql_content) or self.current_database
    namespace = f"mssql://localhost/{inferred_db or self.default_database or 'InfoTrackerDW'}"

    # 1) Check if procedure materializes (SELECT INTO / INSERT INTO ... SELECT)
    materialized_output = self._extract_materialized_output_from_procedure_string(sql_content)
    if materialized_output:
        # Specialized parser: INSERT INTO ... SELECT -> compute lineage from that SELECT
        try:
            ins_lineage, ins_deps = self._extract_insert_select_lineage_string(sql_content, procedure_name)
            if ins_deps:
                materialized_output.dependencies = set(ins_deps)
            if ins_lineage:
                materialized_output.lineage = ins_lineage
        except Exception:
            # Fallback: generic extractor; may include SELECT after INSERT
            try:
                lineage_sel, _, deps_sel = self._extract_procedure_lineage_string(sql_content, procedure_name)
                if deps_sel:
                    materialized_output.dependencies = set(deps_sel)
                if lineage_sel:
                    materialized_output.lineage = lineage_sel
            except Exception:
                basic_deps = self._extract_basic_dependencies(sql_content)
                if basic_deps:
                    materialized_output.dependencies = set(basic_deps)

        # Backfill schema from registry (handle names with/without DB prefix)
        ns = materialized_output.schema.namespace
        name_key = materialized_output.schema.name
        known = None
        if hasattr(self.schema_registry, "get"):
            known = self.schema_registry.get(ns, name_key)
            if not known:
                db = (self.current_database or self.default_database or "InfoTrackerDW")
                parts = name_key.split(".")
                if len(parts) == 2:
                    name_with_db = f"{db}.{name_key}"
                    known = self.schema_registry.get(ns, name_with_db)
        else:
            known = self.schema_registry.get((ns, name_key))

        if known and getattr(known, "columns", None):
            materialized_output.schema = known
        else:
            # Fallback: columns from INSERT INTO column list
            cols = self._extract_insert_into_columns(sql_content)
            if cols:
                materialized_output.schema = TableSchema(
                    namespace=ns,
                    name=name_key,
                    columns=[ColumnSchema(name=c, data_type="unknown", nullable=True, ordinal=i)
                             for i, c in enumerate(cols)]
                )

        # Learn from procedure CREATE only if raw name had explicit DB
        try:
            m = re.search(r'(?is)\bCREATE\s+(?:PROC|PROCEDURE)\s+([^\s(]+)', sql_content)
            raw_ident = m.group(1) if m else ""
            db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
            if self.registry and db_raw:
                self.registry.learn_from_create("procedure", f"{sch_raw}.{tbl_raw}", db_raw)
        except Exception:
            pass
        return materialized_output

    # 2) MERGE INTO ... USING ... as materialized target
    try:
        m_lineage, m_cols, m_deps, m_target = self._extract_merge_lineage_string(sql_content, procedure_name)
    except Exception:
        m_lineage, m_cols, m_deps, m_target = ([], [], set(), None)
    if m_target:
        ns_tgt, nm_tgt = self._ns_and_name(m_target, obj_type_hint="table")
        schema = TableSchema(namespace=ns_tgt, name=nm_tgt, columns=m_cols)
        out_obj = ObjectInfo(
            name=nm_tgt,
            object_type="table",
            schema=schema,
            lineage=m_lineage,
            dependencies=m_deps,
        )
        try:
            m = re.search(r'(?is)\bCREATE\s+(?:PROC|PROCEDURE)\s+([^\s(]+)', sql_content)
            raw_ident = m.group(1) if m else ""
            db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
            if self.registry and db_raw:
                self.registry.learn_from_create("procedure", f"{sch_raw}.{tbl_raw}", db_raw)
        except Exception:
            pass
        return out_obj

    # 2b) UPDATE ... FROM t JOIN s ...
    try:
        u_lineage, u_cols, u_deps, u_target = self._extract_update_from_lineage_string(sql_content)
    except Exception:
        u_lineage, u_cols, u_deps, u_target = ([], [], set(), None)
    if u_target:
        ns_tgt, nm_tgt = self._ns_and_name(u_target, obj_type_hint="table")
        schema = TableSchema(namespace=ns_tgt, name=nm_tgt, columns=u_cols)
        out_obj = ObjectInfo(
            name=nm_tgt,
            object_type="table",
            schema=schema,
            lineage=u_lineage,
            dependencies=u_deps,
        )
        return out_obj

    # 2c) DML with OUTPUT INTO
    try:
        o_lineage, o_cols, o_deps, o_target = self._extract_output_into_lineage_string(sql_content)
    except Exception:
        o_lineage, o_cols, o_deps, o_target = ([], [], set(), None)
    if o_target:
        ns_out, nm_out = self._ns_and_name(o_target, obj_type_hint="table")
        schema = TableSchema(namespace=ns_out, name=nm_out, columns=o_cols)
        out_obj = ObjectInfo(
            name=nm_out,
            object_type="table",
            schema=schema,
            lineage=o_lineage,
            dependencies=o_deps,
        )
        return out_obj

    # 3) If not materializing — last SELECT as virtual dataset of the procedure
    lineage, output_columns, dependencies = self._extract_procedure_lineage_string(sql_content, procedure_name)

    schema = TableSchema(
        namespace=namespace,
        name=procedure_name,
        columns=output_columns
    )

    self.schema_registry.register(schema)

    obj = ObjectInfo(
        name=procedure_name,
        object_type="procedure",
        schema=schema,
        lineage=lineage,
        dependencies=dependencies
    )
    try:
        m = re.search(r'(?is)\bCREATE\s+(?:PROC|PROCEDURE)\s+([^\s(]+)', sql_content)
        raw_ident = m.group(1) if m else ""
        db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
        if self.registry and db_raw:
            self.registry.learn_from_create("procedure", f"{sch_raw}.{tbl_raw}", db_raw)
    except Exception:
        pass
    obj.no_output_reason = "ONLY_PROCEDURE_RESULTSET"
    return obj


def _extract_procedure_name(self, sql_content: str) -> Optional[str]:
    """Extract procedure name from CREATE PROCEDURE statement (string)."""
    match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+([^\s\(]+)', sql_content, re.IGNORECASE)
    return match.group(1).strip() if match else None

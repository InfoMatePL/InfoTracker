from __future__ import annotations

import re
from typing import Optional, List, Set

import sqlglot
from sqlglot import expressions as exp

from ..models import TableSchema, ColumnSchema, ObjectInfo, ColumnLineage


def _parse_create_statement(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
    if statement.kind == "TABLE":
        return _parse_create_table(self, statement, object_hint)
    elif statement.kind == "VIEW":
        return _parse_create_view(self, statement, object_hint)
    elif statement.kind == "FUNCTION":
        return self._parse_create_function(statement, object_hint)
    elif statement.kind == "PROCEDURE":
        return self._parse_create_procedure(statement, object_hint)
    else:
        raise ValueError(f"Unsupported CREATE statement: {statement.kind}")


def _parse_create_table(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
    schema_expr = statement.this
    try:
        raw_ident = schema_expr.this.sql(dialect=self.dialect) if hasattr(schema_expr, 'this') and hasattr(schema_expr.this, 'sql') else str(schema_expr.this)
    except Exception:
        raw_ident = str(schema_expr.this)
    raw_ident = self._normalize_table_ident(raw_ident)
    ns, nm = self._ns_and_name(raw_ident, obj_type_hint="table")
    namespace = ns
    table_name = nm
    explicit_db = False
    try:
        raw_tbl = schema_expr.this
        if isinstance(raw_tbl, exp.Table) and getattr(raw_tbl, 'catalog', None):
            cat = str(raw_tbl.catalog).strip('[]')
            if cat and cat.lower() not in {"view", "function", "procedure", "tempdb"}:
                explicit_db = True
    except Exception:
        pass
    if not explicit_db:
        inferred_db = self._infer_database_for_object(statement=statement, sql_text=getattr(self, "_current_raw_sql", None))
        if inferred_db:
            namespace = f"mssql://localhost/{inferred_db}"
    try:
        db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
        if self.registry and db_raw:
            self.registry.learn_from_create("table", f"{sch_raw}.{tbl_raw}", db_raw)
    except Exception:
        pass

    columns: List[ColumnSchema] = []
    if hasattr(schema_expr, 'expressions') and schema_expr.expressions:
        for i, column_def in enumerate(schema_expr.expressions):
            if isinstance(column_def, exp.ColumnDef):
                col_name = str(column_def.this)
                col_type = self._extract_column_type(column_def)
                nullable = not self._has_not_null_constraint(column_def)
                columns.append(ColumnSchema(name=col_name, data_type=col_type, nullable=nullable, ordinal=i))

    schema = TableSchema(namespace=namespace, name=table_name, columns=columns)
    self.schema_registry.register(schema)
    return ObjectInfo(name=table_name, object_type="table", schema=schema, lineage=[], dependencies=set())


def _parse_create_table_string(self, sql: str, object_hint: Optional[str] = None) -> ObjectInfo:
    m = re.search(r'(?is)CREATE\s+TABLE\s+([^\s(]+)', sql)
    raw_ident = self._normalize_table_ident(m.group(1)) if m else None
    name_for_ns = raw_ident or (object_hint or "dbo.unknown_table")
    ns, nm = self._ns_and_name(name_for_ns, obj_type_hint="table")
    namespace = ns
    table_name = nm
    has_db = bool(raw_ident and raw_ident.count('.') >= 2)
    if not has_db:
        inferred_db = self._infer_database_for_object(statement=None, sql_text=sql)
        if inferred_db:
            namespace = f"mssql://localhost/{inferred_db}"

    cols: List[ColumnSchema] = []
    body_match = re.search(r'(?is)CREATE\s+TABLE\s+[^\(]+\((.*)\)', sql)
    if body_match:
        inner = body_match.group(1)
        parts = re.split(r',(?![^\(]*\))', inner)
        col_lines = [p.strip() for p in parts if p.strip() and not re.match(r'(?i)CONSTRAINT\b', p.strip())]
        for i, ln in enumerate(col_lines):
            m2 = re.match(r'\s*(?:\[([^\]]+)\]|"([^"]+)"|([A-Za-z_][\w$#]*))\s+(.*)$', ln)
            if not m2:
                continue
            col_name = next(g for g in m2.groups()[:3] if g)
            rest = m2.group(4)
            t = re.match(r'(?i)\s*(?:\[(?P<t1>[^\]]+)\]|(?P<t2>[A-Za-z_][\w$]*))\s*(?:\(\s*(?P<args>[^)]*?)\s*\))?', rest)
            if t:
                tname = (t.group('t1') or t.group('t2') or '').upper()
                targs = t.group('args')
                dtype = f"{tname}({targs})" if targs else tname
            else:
                dtype = "UNKNOWN"
            nullable = not re.search(r'(?i)\bNOT\s+NULL\b', rest)
            cols.append(ColumnSchema(name=col_name, data_type=dtype, nullable=nullable, ordinal=i))

    schema = TableSchema(namespace=namespace, name=table_name, columns=cols)
    self.schema_registry.register(schema)
    return ObjectInfo(name=table_name, object_type="table", schema=schema, lineage=[], dependencies=set())


def _parse_create_view(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
    raw_view = self._get_table_name(statement.this, object_hint)
    ns, nm = self._ns_and_name(raw_view, obj_type_hint="view")
    namespace = ns
    view_name = nm
    explicit_db = False
    try:
        raw_tbl = getattr(statement.this, 'this', statement.this)
        if isinstance(raw_tbl, exp.Table) and getattr(raw_tbl, 'catalog', None):
            cat = str(raw_tbl.catalog).strip('[]')
            if cat and cat.lower() not in {"view", "function", "procedure", "tempdb"}:
                explicit_db = True
    except Exception:
        pass
    if not explicit_db:
        inferred_db = self._infer_database_for_object(statement=statement, sql_text=getattr(self, "_current_raw_sql", None))
        if inferred_db:
            namespace = f"mssql://localhost/{inferred_db}"
    try:
        raw_ident = statement.this.sql(dialect=self.dialect) if hasattr(statement, 'this') and hasattr(statement.this, 'sql') else str(statement.this)
        db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
        if self.registry and db_raw:
            self.registry.learn_from_create("view", f"{sch_raw}.{tbl_raw}", db_raw)
    except Exception:
        pass

    view_expr = statement.expression
    if isinstance(view_expr, exp.Select):
        select_stmt = view_expr
    elif isinstance(view_expr, exp.Union):
        select_stmt = view_expr
    else:
        raise ValueError(f"VIEW must contain a SELECT or UNION statement, got {type(view_expr)}")

    if isinstance(select_stmt, exp.Select) and select_stmt.args.get('with'):
        select_stmt = self._process_ctes(select_stmt)

    dependencies = self._extract_dependencies(select_stmt)
    lineage, output_columns = self._extract_column_lineage(select_stmt, view_name)

    if (not lineage) or (not output_columns):
        try:
            sql_text = str(statement)
            m_as = re.search(r"(?is)\bAS\b\s*(.*)$", sql_text)
            if m_as:
                body = m_as.group(1)
                body = self._normalize_tsql(body)
                body = re.sub(r"(?is)^\s*WITH\s+XMLNAMESPACES\s*\(.*?\)\s*", "", body)
                parsed_fallback = sqlglot.parse(body, read=self.dialect)
                sel = None
                if parsed_fallback:
                    for st in parsed_fallback:
                        if isinstance(st, exp.Select):
                            sel = st
                            break
                if sel is not None:
                    dependencies = self._extract_dependencies(sel) or dependencies
                    lineage, output_columns = self._extract_column_lineage(sel, view_name)
                if (not lineage) or (not output_columns):
                    try:
                        m_sel = re.search(r"(?is)\bSELECT\b(.*)$", body)
                        if m_sel:
                            select_sql = "SELECT " + m_sel.group(1)
                            basic_cols = self._extract_basic_select_columns(select_sql)
                            basic_lineage = self._extract_basic_lineage_from_select(select_sql, basic_cols, view_name)
                            if basic_lineage:
                                lineage = basic_lineage
                            if basic_cols:
                                output_columns = basic_cols
                            deps_basic = self._extract_basic_dependencies(select_sql)
                            if deps_basic:
                                dependencies = set(deps_basic)
                    except Exception:
                        pass
        except Exception:
            pass

    schema = TableSchema(namespace=namespace, name=view_name, columns=output_columns)
    self.schema_registry.register(schema)
    obj = ObjectInfo(name=view_name, object_type="view", schema=schema, lineage=lineage, dependencies=dependencies)
    if isinstance(select_stmt, exp.Select):
        self._apply_view_header_names(statement, select_stmt, obj)
    return obj


def _parse_create_function(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
    raw_func = self._get_table_name(statement.this, object_hint)
    ns, nm = self._ns_and_name(raw_func, obj_type_hint="function")
    namespace = ns
    function_name = nm
    explicit_db = False
    try:
        raw_tbl = getattr(statement.this, 'this', statement.this)
        if isinstance(raw_tbl, exp.Table) and getattr(raw_tbl, 'catalog', None):
            cat = str(raw_tbl.catalog).strip('[]')
            if cat and cat.lower() not in {"view", "function", "procedure", "tempdb"}:
                explicit_db = True
    except Exception:
        pass
    if not explicit_db:
        inferred_db = self._infer_database_for_object(statement=statement, sql_text=getattr(self, "_current_raw_sql", None))
        if inferred_db:
            namespace = f"mssql://localhost/{inferred_db}"
    try:
        raw_ident = statement.this.sql(dialect=self.dialect) if hasattr(statement, 'this') and hasattr(statement.this, 'sql') else str(statement.this)
        db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
        if self.registry and db_raw:
            self.registry.learn_from_create("function", f"{sch_raw}.{tbl_raw}", db_raw)
    except Exception:
        pass

    if not self._is_table_valued_function(statement):
        return ObjectInfo(
            name=function_name,
            object_type="function",
            schema=TableSchema(namespace=namespace, name=function_name, columns=[]),
            lineage=[],
            dependencies=set(),
        )

    lineage, output_columns, dependencies = self._extract_tvf_lineage(statement, function_name)
    schema = TableSchema(namespace=namespace, name=function_name, columns=output_columns)
    self.schema_registry.register(schema)
    return ObjectInfo(name=function_name, object_type="function", schema=schema, lineage=lineage, dependencies=dependencies)


def _parse_create_procedure(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
    raw_proc = self._get_table_name(statement.this, object_hint)
    ns, nm = self._ns_and_name(raw_proc, obj_type_hint="procedure")
    namespace = ns
    procedure_name = nm
    explicit_db = False
    try:
        raw_tbl = getattr(statement.this, 'this', statement.this)
        if isinstance(raw_tbl, exp.Table) and getattr(raw_tbl, 'catalog', None):
            cat = str(raw_tbl.catalog).strip('[]')
            if cat and cat.lower() not in {"view", "function", "procedure", "tempdb"}:
                explicit_db = True
    except Exception:
        pass
    if not explicit_db:
        inferred_db = self._infer_database_for_object(statement=statement, sql_text=getattr(self, "_current_raw_sql", None))
        if inferred_db:
            namespace = f"mssql://localhost/{inferred_db}"
    try:
        raw_ident = statement.this.sql(dialect=self.dialect) if hasattr(statement, 'this') and hasattr(statement.this, 'sql') else str(statement.this)
        db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
        if self.registry and db_raw:
            self.registry.learn_from_create("procedure", f"{sch_raw}.{tbl_raw}", db_raw)
    except Exception:
        pass

    materialized_outputs = self._extract_procedure_outputs(statement)
    if not materialized_outputs:
        try:
            m_lineage, m_cols, m_deps, m_target = self._extract_merge_lineage_string(str(statement), procedure_name)
        except Exception:
            m_lineage, m_cols, m_deps, m_target = ([], [], set(), None)
        if m_target:
            ns_tgt, nm_tgt = self._ns_and_name(m_target, obj_type_hint="table")
            schema = TableSchema(namespace=namespace or ns_tgt, name=nm_tgt, columns=m_cols)
            out_obj = ObjectInfo(name=nm_tgt, object_type="table", schema=schema, lineage=m_lineage, dependencies=m_deps)
            return out_obj

    if materialized_outputs:
        last_output = materialized_outputs[-1]
        lineage, output_columns, dependencies = self._extract_procedure_lineage(statement, procedure_name)
        last_output.lineage = lineage
        last_output.dependencies = dependencies
        if last_output.schema:
            last_output.schema.namespace = namespace
            last_output.schema.name = self._normalize_table_name_for_output(last_output.schema.name)
        last_output.name = last_output.schema.name if last_output.schema else last_output.name
        if output_columns:
            last_output.schema = TableSchema(namespace=last_output.schema.namespace, name=last_output.name, columns=output_columns)
        return last_output

    lineage, output_columns, dependencies = self._extract_procedure_lineage(statement, procedure_name)
    schema = TableSchema(namespace=namespace, name=procedure_name, columns=output_columns)
    self.schema_registry.register(schema)
    obj = ObjectInfo(name=procedure_name, object_type="procedure", schema=schema, lineage=lineage, dependencies=dependencies)
    obj.no_output_reason = "ONLY_PROCEDURE_RESULTSET"
    return obj


def _extract_procedure_outputs(self, statement: exp.Create) -> List[ObjectInfo]:
    """Extract materialized outputs (SELECT INTO, INSERT INTO) from procedure body."""
    outputs: List[ObjectInfo] = []
    sql_text = str(statement)

    # SELECT ... INTO <table>
    select_into_pattern = r'(?i)SELECT\s+.*?\s+INTO\s+([^\s,]+)'
    select_into_matches = re.findall(select_into_pattern, sql_text, re.DOTALL)
    for table_match in select_into_matches:
        table_name = table_match.strip()
        if not table_name.startswith('#') and 'tempdb' not in table_name.lower():
            normalized_name = self._normalize_table_name_for_output(table_name)
            db = self.current_database or self.default_database or "InfoTrackerDW"
            outputs.append(ObjectInfo(
                name=normalized_name,
                object_type="table",
                schema=TableSchema(
                    namespace=f"mssql://localhost/{db}",
                    name=normalized_name,
                    columns=[]
                ),
                lineage=[],
                dependencies=set()
            ))

    # INSERT INTO <table>
    insert_into_pattern = r'(?i)INSERT\s+INTO\s+([^\s,\(]+)'
    insert_into_matches = re.findall(insert_into_pattern, sql_text)
    for table_match in insert_into_matches:
        table_name = table_match.strip()
        if not table_name.startswith('#') and 'tempdb' not in table_name.lower():
            normalized_name = self._normalize_table_name_for_output(table_name)
            if not any(output.name == normalized_name for output in outputs):
                db = self.current_database or self.default_database or "InfoTrackerDW"
                outputs.append(ObjectInfo(
                    name=normalized_name,
                    object_type="table",
                    schema=TableSchema(
                        namespace=f"mssql://localhost/{db}",
                        name=normalized_name,
                        columns=[]
                    ),
                    lineage=[],
                    dependencies=set()
                ))

    return outputs


def _extract_tvf_lineage(self, statement: exp.Create, function_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
    """Extract lineage from a table-valued function (AST-based)."""
    lineage: List[ColumnLineage] = []
    output_columns: List[ColumnSchema] = []
    dependencies: Set[str] = set()

    sql_text = str(statement)

    # Inline TVF: RETURN AS (SELECT ...)
    if "RETURN AS" in sql_text.upper() or "RETURN(" in sql_text.upper():
        select_stmt = self._extract_select_from_return(statement)
        if select_stmt:
            self._process_ctes(select_stmt)
            lineage, output_columns = self._extract_column_lineage(select_stmt, function_name)
            raw_deps = self._extract_dependencies(select_stmt)
            for dep in raw_deps:
                expanded = self._expand_dependency_to_base_tables(dep, select_stmt)
                dependencies.update(expanded)

    # Multi-statement TVF: RETURNS @table TABLE ...
    elif "RETURNS @" in sql_text.upper():
        output_columns = self._extract_table_variable_schema(statement)
        lineage, raw_deps = self._extract_mstvf_lineage(statement, function_name, output_columns)
        for dep in raw_deps:
            expanded = self._expand_dependency_to_base_tables(dep, statement)
            dependencies.update(expanded)

    # Fallback if AST path failed
    if not dependencies and not lineage:
        try:
            lineage, output_columns, dependencies = self._extract_tvf_lineage_string(sql_text, function_name)
        except Exception:
            pass

    dependencies = {d for d in dependencies if not self._is_cte_reference(d)}
    return lineage, output_columns, dependencies


def _extract_procedure_lineage(self, statement: exp.Create, procedure_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
    """Extract lineage from a procedure that returns a dataset (AST-based)."""
    lineage: List[ColumnLineage] = []
    output_columns: List[ColumnSchema] = []
    dependencies: Set[str] = set()

    last_select = self._find_last_select_in_procedure(statement)
    if last_select:
        lineage, output_columns = self._extract_column_lineage(last_select, procedure_name)
        dependencies = self._extract_dependencies(last_select)
    return lineage, output_columns, dependencies


def _extract_select_from_return(self, statement: exp.Create) -> Optional[exp.Select]:
    """Extract SELECT statement from RETURN AS clause."""
    try:
        sql_text = str(statement)
        m = re.search(r'RETURN\s*\(\s*(SELECT.*?)\s*\)', sql_text, re.IGNORECASE | re.DOTALL)
        if m:
            select_sql = m.group(1)
            parsed = sqlglot.parse(select_sql, read=self.dialect)
            if parsed and isinstance(parsed[0], exp.Select):
                return parsed[0]
    except Exception:
        pass
    return None


def _extract_table_variable_schema(self, statement: exp.Create) -> List[ColumnSchema]:
    """Extract column schema from @table TABLE definition (AST context, regex parsing)."""
    output_columns: List[ColumnSchema] = []
    sql_text = str(statement)
    m = re.search(r'@\w+\s+TABLE\s*\((.*?)\)', sql_text, re.IGNORECASE | re.DOTALL)
    if not m:
        return output_columns
    columns_def = m.group(1)
    for i, col_def in enumerate(columns_def.split(',')):
        parts = col_def.strip().split()
        if len(parts) >= 2:
            col_name = parts[0].strip()
            col_type = parts[1].strip()
            output_columns.append(ColumnSchema(
                name=col_name,
                data_type=col_type,
                nullable=True,
                ordinal=i,
            ))
    return output_columns

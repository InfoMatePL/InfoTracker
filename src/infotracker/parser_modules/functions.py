from __future__ import annotations
from typing import Optional, List, Set
import re

from sqlglot import exp  # type: ignore

from ..models import ObjectInfo, TableSchema, ColumnSchema, ColumnLineage
import sqlglot


def _parse_function_string(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
    """Parse CREATE FUNCTION using string-based approach (extracted)."""
    function_name = self._extract_function_name(sql_content) or object_hint or "unknown_function"
    inferred_db = self._infer_database_for_object(statement=None, sql_text=sql_content) or self.current_database or self.default_database
    namespace = self._canonical_namespace(inferred_db)

    # Establish ctx (db + object) for canonical temp names inside TVF processing
    prev_ctx_db, prev_ctx_obj = getattr(self, "_ctx_db", None), getattr(self, "_ctx_obj", None)
    self._ctx_db = inferred_db or self.current_database or self.default_database
    self._ctx_obj = self._normalize_table_name_for_output(function_name)

    # Scalar function: no lineage, empty schema
    if not self._is_table_valued_function_string(sql_content):
        obj = ObjectInfo(
            name=function_name,
            object_type="function",
            schema=TableSchema(
                namespace=namespace,
                name=function_name,
                columns=[]
            ),
            lineage=[],
            dependencies=set()
        )
        try:
            m = re.search(r'(?is)\bCREATE\s+FUNCTION\s+([^\s(]+)', sql_content)
            raw_ident = m.group(1) if m else ""
            db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
            if self.registry and db_raw:
                self.registry.learn_from_create("function", f"{sch_raw}.{tbl_raw}", db_raw)
        except Exception:
            pass
        # restore ctx
        self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
        return obj

    # Table-valued function: compute lineage
    lineage, output_columns, dependencies = self._extract_tvf_lineage_string(sql_content, function_name)

    schema = TableSchema(
        namespace=namespace,
        name=function_name,
        columns=output_columns
    )

    self.schema_registry.register(schema)

    obj = ObjectInfo(
        name=function_name,
        object_type="function",
        schema=schema,
        lineage=lineage,
        dependencies=dependencies
    )
    try:
        m = re.search(r'(?is)\bCREATE\s+FUNCTION\s+([^\s(]+)', sql_content)
        raw_ident = m.group(1) if m else ""
        db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
        if self.registry and db_raw:
            self.registry.learn_from_create("function", f"{sch_raw}.{tbl_raw}", db_raw)
    except Exception:
        pass
    # restore ctx
    self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
    return obj


def _extract_select_from_return_string(self, sql_content: str) -> Optional[str]:
    """Extract SELECT statement from RETURN clause using enhanced regex (shared with string_fallbacks)."""
    # Remove comments first
    cleaned_sql = re.sub(r'--.*?(?=\n|$)', '', sql_content, flags=re.MULTILINE)
    cleaned_sql = re.sub(r'/\*.*?\*/', '', cleaned_sql, flags=re.DOTALL)

    patterns = [
        r'RETURNS\s+TABLE\s+AS\s+RETURN\s*\(\s*(SELECT.*?)(?=\)[\s;]*(?:END|$))',
        r'RETURNS\s+TABLE\s+RETURN\s*\(\s*(SELECT.*?)(?=\)[\s;]*(?:END|$))',
        r'RETURNS\s+TABLE\s+RETURN\s+(SELECT.*?)(?=[\s;]*(?:END|$))',
        r'RETURN\s+AS\s*\n\s*\(\s*(SELECT.*?)(?=\)[\s;]*(?:END|$))',
        r'RETURN\s*\n\s*\(\s*(SELECT.*?)(?=\)[\s;]*(?:END|$))',
        r'RETURN\s+AS\s*\(\s*(SELECT.*?)(?=\)[\s;]*(?:END|$))',
        r'RETURN\s*\(\s*(SELECT.*?)(?=\)[\s;]*(?:END|$))',
        r'AS\s*\n\s*RETURN\s*\n\s*\(\s*(SELECT.*?)(?=\)[\s;]*(?:END|$))',
        r'RETURN\s+(SELECT.*?)(?=[\s;]*(?:END|$))',
        r'RETURN\s*\(\s*(SELECT.*?)\s*\)(?:\s*;)?$'
    ]

    for pattern in patterns:
        match = re.search(pattern, cleaned_sql, re.DOTALL | re.IGNORECASE)
        if match:
            select_statement = match.group(1).strip()
            if select_statement.upper().strip().startswith('SELECT'):
                return select_statement
    return None


def _is_table_valued_function_string(self, sql_content: str) -> bool:
    """Check if this is a table-valued function (returns TABLE)."""
    sql_upper = sql_content.upper()
    return "RETURNS TABLE" in sql_upper or "RETURNS @" in sql_upper


def _extract_function_name(self, sql_content: str) -> Optional[str]:
    """Extract function name from CREATE FUNCTION statement (string)."""
    match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?FUNCTION\s+([^\s\(]+)', sql_content, re.IGNORECASE)
    return match.group(1).strip() if match else None

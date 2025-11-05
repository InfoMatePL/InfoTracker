from __future__ import annotations

import re
from typing import Optional
from sqlglot import expressions as exp


def _rewrite_case_with_commas_to_iif(sql: str) -> str:
    """Rewrite only the non-standard "CASE WHEN cond, true, false END" form to IIF(cond, true, false).

    Safety: do NOT touch standard CASE WHEN ... THEN ... [WHEN ... THEN ...] END blocks.
    """
    if not sql:
        return sql

    def _safe_repl(m: re.Match) -> str:
        whole = m.group(0) or ""
        if re.search(r"(?i)\bTHEN\b", whole):
            return whole
        cond = (m.group("cond") or "").strip()
        t = (m.group("t") or "").strip()
        f = (m.group("f") or "").strip()
        return f"IIF({cond}, {t}, {f})"

    pat_paren = re.compile(
        r"""
        CASE\s+WHEN\s+
        (?P<cond>[^,()]+(?:\([^)]*\)[^,()]*)*)\s*,\s*
        (?P<t>[^,()]+(?:\([^)]*\)[^,()]*)*)\s*,\s*
        (?P<f>[^)]+?)\s*\)
        """,
        re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )

    pat_end = re.compile(
        r"""
        CASE\s+WHEN\s+
        (?P<cond>[^,()]+(?:\([^)]*\)[^,()]*)*)\s*,\s*
        (?P<t>[^,()]+(?:\([^)]*\)[^,()]*)*)\s*,\s*
        (?P<f>[^)]+?)\s*END
        """,
        re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )

    out = pat_paren.sub(_safe_repl, sql)
    out = pat_end.sub(_safe_repl, out)
    return out


def _strip_udf_options_between_returns_and_as(sql: str) -> str:
    """Strip UDF options between RETURNS ... and AS.

    - TVF (RETURNS TABLE ... AS): remove options between TABLE and AS
    - Scalar UDF (RETURNS <type> WITH ... AS): remove WITH ... up to AS
    """
    pat_tvf = re.compile(
        r"""
        (?P<head>\bRETURNS\b\s+TABLE)
        (?P<middle>(?!\s*AS\b)[\s\S]*?)
        \bAS\b
        """,
        re.IGNORECASE | re.VERBOSE,
    )
    def _repl_tvf(m: re.Match) -> str:
        return f"{m.group('head')}\nAS"
    out = pat_tvf.sub(_repl_tvf, sql or "")
    pat_scalar = re.compile(
        r"""
        (?P<head>\bRETURNS\b\s+(?!TABLE\b)[\w\[\]]+(?:\s*\([^)]*\))?)
        \s+(?P<opts>WITH\b[\s\S]*?)
        \bAS\b
        """,
        re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )
    out = pat_scalar.sub(lambda m: f"{m.group('head')}\nAS", out)
    return out


def _normalize_tsql(self, text: str) -> str:
    """Normalize T-SQL to improve sqlglot parsing compatibility."""
    t = (text or "").replace("\r\n", "\n")
    try:
        t = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", t)
        t = re.sub(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]", "", t)
    except Exception:
        pass
    t = re.sub(r"^\s*SET\s+(ANSI_NULLS|QUOTED_IDENTIFIER)\s+(ON|OFF)\s*;?\s*$", "", t, flags=re.I|re.M)
    t = re.sub(r"^\s*GO\s*;?\s*$", "", t, flags=re.I|re.M)
    t = re.sub(r"\s+COLLATE\s+[A-Za-z0-9_]+", "", t, flags=re.I)
    t = re.sub(r"\bISNULL\s*\(", "COALESCE(", t, flags=re.I)
    try:
        t = re.sub(r"[\u200B\u200C\u200D\u00A0]", " ", t)
    except Exception:
        pass
    try:
        t = re.sub(r"\bWITH\s+XMLNAMESPACES\s*\(.*?\)\s*", "", t, flags=re.IGNORECASE | re.DOTALL)
    except Exception:
        pass
    return t


def _extract_dbt_model_name(self, sql_text: str) -> Optional[str]:
    """Extract dbt model logical name from leading comment, e.g.:
    -- dbt model: stg_orders
    Returns lowercased sanitized name or None if not found.
    """
    try:
        head = "\n".join((sql_text or "").splitlines()[:8])
        m = re.search(r"(?im)^\s*--\s*dbt\s+model:\s*([A-Za-z0-9_\.]+)", head)
        if m:
            name = (m.group(1) or "").strip()
            name = name.split('.')[-1]
            from ..openlineage_utils import sanitize_name
            return sanitize_name(name)
    except Exception:
        pass
    return None


def _extract_database_from_use_statement(self, content: str) -> Optional[str]:
    lines = (content or "").strip().split('\n')
    for line in lines[:10]:
        line = line.strip()
        if not line or line.startswith('--'):
            continue
        use_match = re.match(r'USE\s+(?::([^:]+):|(?:\[([^\]]+)\]|(\w+)))', line, re.IGNORECASE)
        if use_match:
            db_name = use_match.group(1) or use_match.group(2) or use_match.group(3)
            try:
                self._log_debug(f"Found USE statement, setting database to: {db_name}")
            except Exception:
                pass
            return db_name
        if not line.upper().startswith(('USE', 'DECLARE', 'SET', 'PRINT')):
            break
    return None


def _cut_to_first_statement(self, sql: str) -> str:
    pattern = re.compile(
        r'(?:'
        r'CREATE\s+(?:OR\s+ALTER\s+)?(?:VIEW|TABLE|FUNCTION|PROCEDURE)\b'
        r'|ALTER\s+(?:VIEW|TABLE|FUNCTION|PROCEDURE)\b'
        r'|SELECT\b.*?\bINTO\b'
        r'|INSERT\s+INTO\b.*?\bEXEC\b'
        r')',
        re.IGNORECASE | re.DOTALL
    )
    m = pattern.search(sql or "")
    return (sql or "")[m.start():] if m else (sql or "")


def _preprocess_sql(self, sql: str) -> str:
    db_from_use = _extract_database_from_use_statement(self, sql)
    if db_from_use:
        self.current_database = db_from_use
    else:
        self.current_database = self.default_database

    lines = (sql or "").split('\n')
    processed_lines = []
    for line in lines:
        stripped_line = line.strip()
        if re.match(r'(?i)^(DECLARE|SET|PRINT)\b', stripped_line):
            continue
        if (re.match(r"(?i)^IF\s+OBJECT_ID\('tempdb\.\.#", stripped_line) or
            re.match(r'(?i)^DROP\s+TABLE\s+#\w+', stripped_line) or
            re.match(r'(?i)^IF\s+OBJECT_ID.*IS\s+NOT\s+NULL\s+DROP\s+TABLE', stripped_line)):
            continue
        if re.match(r'(?im)^\s*GO\s*$', stripped_line):
            continue
        if re.match(r'(?i)^\s*USE\b', stripped_line):
            continue
        processed_lines.append(line)

    processed_sql = '\n'.join(processed_lines)
    processed_sql = re.sub(r'(INSERT\s+INTO\s+#\w+)\s*\n\s*(EXEC\b)', r'\1 \2', processed_sql, flags=re.IGNORECASE)
    processed_sql = _cut_to_first_statement(self, processed_sql)
    try:
        processed_sql = _strip_udf_options_between_returns_and_as(processed_sql)
        processed_sql = _rewrite_case_with_commas_to_iif(processed_sql)
    except Exception:
        pass
    return processed_sql


def _rewrite_ast(self, root: Optional[exp.Expression]) -> Optional[exp.Expression]:
    """Rewrite AST nodes for better T-SQL compatibility."""
    if root is None:
        return None
    for node in list(root.walk()):
        # Convert CONVERT(T, x [, style]) to CAST(x AS T)
        if isinstance(node, exp.Convert):
            target_type = node.args.get("to")
            source_expr = node.args.get("expression")
            if target_type and source_expr:
                cast_node = exp.Cast(this=source_expr, to=target_type)
                node.replace(cast_node)

        # Mark HASHBYTES(...) nodes for special handling
        if isinstance(node, exp.Anonymous) and (node.name or "").upper() == "HASHBYTES":
            node.set("is_hashbytes", True)

    return root

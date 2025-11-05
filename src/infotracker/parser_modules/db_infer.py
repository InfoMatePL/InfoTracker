from __future__ import annotations

import re
from typing import Optional
from sqlglot import expressions as exp


def _strip_sql_comments(self, sql: str) -> str:
    if not sql:
        return sql
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql


def _infer_db_candidates_from_ast(self, node):
    from collections import Counter
    c = Counter()
    if not node:
        return c
    for t in node.find_all(exp.Table):
        cat = (str(t.catalog) if t.catalog else "").strip('[]').strip()
        if not cat:
            continue
        cl = cat.lower()
        if cl in {"view", "function", "procedure", "tempdb"}:
            continue
        c[cat] += 1
    for ins in node.find_all(exp.Insert):
        tbl = ins.this
        if isinstance(tbl, exp.Table) and tbl.catalog:
            cat = str(tbl.catalog).strip('[]')
            if cat and cat.lower() not in {"view", "function", "procedure", "tempdb"}:
                c[cat] += 3
    return c


def _infer_db_candidates_from_sql(self, sql_text: str):
    from collections import Counter
    c = Counter()
    if not sql_text:
        return c
    sql = _strip_sql_comments(self, sql_text)
    for m in re.finditer(r'([A-Za-z_][A-Za-z0-9_\[\]]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_\[\]]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_\[\]]*)', sql):
        db = m.group(1).strip('[]')
        if db.lower() in {"view", "function", "procedure", "tempdb"}:
            continue
        c[db] += 1
    for m in re.finditer(r'(?i)\bINSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_\[\]]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_\[\]]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_\[\]]*)', sql):
        db = m.group(1).strip('[]')
        if db.lower() in {"view", "function", "procedure", "tempdb"}:
            continue
        c[db] += 3
    for m in re.finditer(r'(?is)\bSELECT\b.*?\bINTO\s+([A-Za-z_][A-Za-z0-9_\[\]]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_\[\]]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_\[\]]*)', sql):
        db = m.group(1).strip('[]')
        if db.lower() in {"view", "function", "procedure", "tempdb"}:
            continue
        c[db] += 3
    return c


def _choose_db(self, counter) -> Optional[str]:
    if not counter:
        return None
    mc = counter.most_common()
    if len(mc) == 1 or (len(mc) > 1 and mc[0][1] > mc[1][1]):
        return mc[0][0]
    return None


def _infer_database_for_object(self, statement=None, sql_text: Optional[str] = None) -> Optional[str]:
    from collections import Counter
    c = Counter()
    try:
        if statement is not None:
            c += _infer_db_candidates_from_ast(self, statement)
    except Exception:
        pass
    try:
        c += _infer_db_candidates_from_sql(self, sql_text or "")
    except Exception:
        pass
    db = _choose_db(self, c)
    if db:
        return db
    return self.current_database or self.default_database or None

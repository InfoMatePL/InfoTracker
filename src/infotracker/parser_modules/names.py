from __future__ import annotations

import re
from typing import Optional, Tuple
from functools import lru_cache
from sqlglot import expressions as exp
from ..openlineage_utils import qualify_identifier, sanitize_name


@lru_cache(maxsize=65536)
def _cached_split_fqn_core(fqn: str):
    parts = (fqn or "").split(".")
    if len(parts) >= 3:
        return parts[0], parts[1], ".".join(parts[2:])
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, "dbo", (parts[0] if parts else None)


def _clean_proc_name(self, s: str) -> str:
    """Clean procedure name by removing semicolons and parameters."""
    try:
        return (s or "").strip().rstrip(';').split('(')[0].strip()
    except Exception:
        return (s or "").strip()


def _normalize_table_ident(self, s: str) -> str:
    """Remove brackets and normalize table identifier."""
    try:
        normalized = re.sub(r"[\[\]]", "", (s or ""))
        return normalized.strip().rstrip(';')
    except Exception:
        return (s or "").strip()


def _split_fqn(self, fqn: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Split fully qualified name into (db, schema, table) using cached core and context default."""
    db, sch, tbl = _cached_split_fqn_core(fqn)
    if db is None:
        db = self.current_database or self.default_database
    return db, sch, tbl


def _ns_and_name(self, table_name: str, obj_type_hint: str = "table") -> tuple[str, str]:
    if table_name and (table_name.startswith('#') or 'tempdb..#' in table_name):
        return "mssql://localhost/tempdb", table_name
    raw_parts = (table_name or "").split('.')
    parts = [p for p in raw_parts if p != ""]
    pseudo = {"view", "function", "procedure"}
    if len(parts) >= 3 and parts[0].lower() in pseudo:
        parts = parts[1:]
    if getattr(self, 'dbt_mode', False):
        last = parts[-1] if parts else table_name
        db = self.current_database or self.default_database or "InfoTrackerDW"
        # dbt-mode keeps db name case as provided by project.yml/tests
        ns = f"mssql://localhost/{db}"
        nm = f"{self.default_schema or 'dbo'}.{last}"
        return ns, nm
    db: Optional[str]
    if len(parts) >= 3:
        db = parts[0]
    else:
        db = None
        schema_table = None
        if len(parts) >= 2:
            schema_table = ".".join(parts[-2:])
        elif len(parts) == 1 and parts[0]:
            schema_table = f"dbo.{parts[0]}"
        if getattr(self, 'registry', None) and schema_table:
            fallback = self.current_database or self.default_database or "InfoTrackerDW"
            db = self.registry.resolve(obj_type_hint or "table", schema_table, fallback=fallback)
        if not db:
            db = self.current_database or self.default_database or "InfoTrackerDW"
    # In classic mode, canonicalize DB casing to avoid duplicate namespaces
    ns = f"mssql://localhost/{str(db).upper()}"
    if len(parts) >= 2:
        nm = ".".join(parts[-2:])
    elif len(parts) == 1 and parts[0]:
        nm = f"dbo.{parts[0]}"
    else:
        nm = table_name
    return ns, nm


def _qualify_table(self, tbl: exp.Table) -> str:
    name = tbl.name
    sch = getattr(tbl, "db", None) or "dbo"
    db = getattr(tbl, "catalog", None) or self.current_database or self.default_database
    return ".".join([p for p in [db, sch, name] if p])


def _get_table_name(self, table_expr: exp.Expression, hint: Optional[str] = None) -> str:
    """Extract table name from expression and qualify with current or default database."""
    database_to_use = self.current_database or self.default_database
    if isinstance(table_expr, exp.Table):
        # sqlglot drops the leading '#' from temp table identifiers in T-SQL.
        # Detect temps via context:
        #  - catalog == tempdb means it's a temp (restore '#')
        #  - simple name present in temp_registry (as '#name')
        try:
            simple = str(table_expr.name)
            if getattr(table_expr, 'catalog', None):
                cat = str(table_expr.catalog)
                if cat and cat.lower() == 'tempdb':
                    return f"tempdb..#{simple}"
            # If we have materialized this temp earlier in the procedure, map it to tempdb canonical form
            if simple and (f"#{simple}" in self.temp_registry):
                return f"tempdb..#{simple}"
        except Exception:
            pass
        catalog = str(table_expr.catalog) if table_expr.catalog else None
        if catalog and catalog.lower() in {"view", "function", "procedure"}:
            catalog = None
        if catalog and table_expr.db:
            full_name = f"{catalog}.{table_expr.db}.{table_expr.name}"
        elif table_expr.db:
            table_name = f"{table_expr.db}.{table_expr.name}"
            full_name = qualify_identifier(table_name, database_to_use)
        else:
            table_name = str(table_expr.name)
            full_name = qualify_identifier(table_name, database_to_use)
    elif isinstance(table_expr, exp.Identifier):
        # Identifiers may also point at temps without leading '#'. If present in temp_registry, restore it.
        try:
            ident = str(table_expr.this)
            if ident and (f"#{ident}" in self.temp_registry):
                return f"tempdb..#{ident}"
        except Exception:
            pass
        table_name = str(table_expr.this)
        full_name = qualify_identifier(table_name, database_to_use)
    else:
        full_name = hint or "unknown"
    if full_name and full_name.startswith('#'):
        temp_name = full_name.lstrip('#')
        return f"tempdb..#{temp_name}"
    return sanitize_name(full_name)


def _get_full_table_name(self, table_name: str) -> str:
    """Get full table name with database prefix using current or default database.
    Rules:
    - name -> db.dbo.name
    - schema.table -> db.schema.table
    - db.schema.table -> as-is
    """
    db_to_use = self.current_database or self.default_database or "InfoTrackerDW"
    parts = (table_name or "").split('.')
    parts = [p for p in parts if p != ""]
    if len(parts) == 1:
        return f"{db_to_use}.dbo.{parts[0]}"
    if len(parts) == 2:
        return f"{db_to_use}.{parts[0]}.{parts[1]}"
    return sanitize_name(".".join(parts[:3]))


def _normalize_table_name_for_output(self, table_name: str) -> str:
    """Normalize name for output: drop DB if present, ensure schema.table."""
    table_name = sanitize_name(table_name)
    parts = (table_name or "").split('.')
    if len(parts) >= 3:
        return f"{parts[-2]}.{parts[-1]}"
    if len(parts) == 2:
        return table_name
    return f"dbo.{table_name}"


def _get_namespace_for_table(self, table_name: str) -> str:
    """Return OpenLineage namespace for a given table-like string.

    Canonicalization rules:
    - temp tables -> tempdb namespace
    - dbt mode keeps DB case as provided
    - classic mode uppercases DB to avoid duplicate buckets differing only by case
    """
    if table_name.startswith('#') or table_name.startswith('tempdb..#'):
        return "mssql://localhost/tempdb"
    db = self.current_database or self.default_database or "InfoTrackerDW"
    if getattr(self, 'dbt_mode', False):
        return f"mssql://localhost/{db}"
    return f"mssql://localhost/{str(db).upper()}"

def _canonical_namespace(self, db: str | None) -> str:
    """Build a canonical namespace string for the current parser mode.

    In dbt mode we preserve the provided case (tests rely on it). In classic mode
    we uppercase to collapse duplicates that differ only by case.
    """
    if not db:
        db = "InfoTrackerDW"
    if getattr(self, 'dbt_mode', False):
        return f"mssql://localhost/{db}"
    return f"mssql://localhost/{str(db).upper()}"

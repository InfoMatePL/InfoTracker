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
    # SQL keywords that should never be treated as table names
    JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}
    
    # Check if table_name is a SQL keyword - return empty namespace and keyword as-is
    parts_check = (table_name or "").split('.')
    if parts_check and parts_check[-1].lower() in JOIN_KEYWORDS:
        # Return a marker that this is not a valid table
        return "", "unknown"
    
    # Check if this is a CTE reference - CTEs don't need database qualification
    # They exist only in the query context, not in the database
    simple_name = parts_check[-1] if parts_check else table_name
    if simple_name and simple_name in self.cte_registry:
        # CTEs are query-scoped, but give them a namespace for visualization
        # Use current database namespace to group them properly
        db = self.current_database or self.default_database or "InfoTrackerDW"
        ns = f"mssql://localhost/{db}"
        # Prefix CTE name with schema for proper grouping in visualization
        schema = getattr(self, '_ctx_schema', None) or self.default_schema or "dbo"
        name = f"{schema}.{simple_name}"
        return ns, name
    
    if table_name and (table_name.startswith('#') or '.#' in table_name or 'tempdb..#' in table_name or 'tempdb' in table_name.lower() or ('[' in table_name and '#' in table_name)):
        # If table_name is already canonical (contains '.#'), use it directly
        # Otherwise, get canonical temp name which includes procedure context
        # Format: DB.schema.object.#temp[@v] or just #temp[@v] if no context
        if '.#' in table_name or 'dbo.update_' in table_name:
            # Already canonical, don't call _canonical_temp_name again
            canonical = table_name
        else:
            try:
                canonical = self._canonical_temp_name(table_name)
            except Exception:
                canonical = table_name
        
        # Parse canonical name to extract DB, schema, object, and temp name
        # Format: EDW_CORE.dbo.update_asefl_TrialBalance_BV.#asefl_temp
        # Or: #asefl_temp (if no context)
        if '.' in canonical:
            parts = canonical.split('.')
            # Find the part with '#' (temp table name)
            temp_idx = -1
            for i, part in enumerate(parts):
                if '#' in part:
                    temp_idx = i
                    break
            
            if temp_idx >= 0:
                # Extract temp name (may include version like #temp@1)
                temp_part = parts[temp_idx]
                # Remove square brackets if present
                temp_part = temp_part.strip('[]')
                if not temp_part.startswith('#'):
                    temp_part = f"#{temp_part.lstrip('#')}"
                
                if temp_idx >= 2:
                    # Full format: DB.schema.object.#temp or DB.schema.#temp
                    db = parts[0]
                    schema = parts[1]
                    # If temp_idx > 2, there's an object name between schema and temp
                    if temp_idx > 2:
                        obj_name = '.'.join(parts[2:temp_idx])
                        # Combine schema.object#temp (without dot before #)
                        name = f"{schema}.{obj_name}{temp_part}"
                    else:
                        # Format: DB.schema.#temp (no object name)
                        name = f"{schema}.{temp_part}"
                    return f"mssql://localhost/{db.upper()}", name
                elif temp_idx == 1:
                    # Format: schema.#temp
                    db = self.current_database or self.default_database or "InfoTrackerDW"
                    schema = parts[0]
                    name = f"{schema}.{temp_part}"
                    return f"mssql://localhost/{db.upper()}", name
        
        # Simple case: #temp (no context or parsing failed)
        db = self.current_database or self.default_database or "InfoTrackerDW"
        clean_temp = canonical.strip('[]')
        if not clean_temp.startswith('#'):
            clean_temp = f"#{clean_temp.lstrip('#')}"
        return f"mssql://localhost/{db.upper()}", f"dbo.{clean_temp}"
    raw_parts = (table_name or "").split('.')
    parts = [p for p in raw_parts if p != ""]
    pseudo = {"view", "function", "procedure", "table", "storedprocedure"}
    if len(parts) >= 3 and parts[0].lower() in pseudo:
        parts = parts[1:]
    if getattr(self, 'dbt_mode', False):
        last = parts[-1] if parts else table_name
        db = self.current_database or self.default_database or "InfoTrackerDW"
        # dbt-mode keeps db name case as provided by project.yml/tests
        ns = f"mssql://localhost/{db}"
        nm = f"{self.default_schema or 'dbo'}.{last}"
        return ns, nm
    
    # Classic mode: determine database and schema based on qualification level
    db: Optional[str]
    schema: Optional[str]
    name: Optional[str]
    
    if len(parts) >= 3:
        # baza.schemat.nazwa - use all three parts explicitly
        db = parts[0]
        schema = parts[1]
        name = parts[2]
    elif len(parts) == 2:
        # schemat.nazwa - database is same as currently parsed object (from USE statement)
        db = self.current_database or self.default_database or "InfoTrackerDW"
        schema = parts[0]
        name = parts[1]
    elif len(parts) == 1 and parts[0]:
        # nazwa - database and schema are same as currently parsed object
        db = self.current_database or self.default_database or "InfoTrackerDW"
        schema = getattr(self, '_ctx_schema', None) or self.default_schema or "dbo"
        name = parts[0]
    else:
        # fallback
        db = self.current_database or self.default_database or "InfoTrackerDW"
        schema = "dbo"
        name = table_name
    
    # In classic mode, canonicalize DB casing to avoid duplicate namespaces
    ns = f"mssql://localhost/{str(db).upper()}"
    nm = f"{schema}.{name}" if schema and name else table_name
    return ns, nm


def _qualify_table(self, tbl: exp.Table) -> str:
    name = tbl.name
    sch = getattr(tbl, "db", None) or "dbo"
    db = getattr(tbl, "catalog", None) or self.current_database or self.default_database
    
    # For temp tables, use canonical name instead of tempdb
    # sqlglot parses #temp as catalog=tempdb, but we want EDW_CORE.dbo.procedure.#temp
    if name and name.startswith('#'):
        try:
            canonical = self._canonical_temp_name(name)
            # canonical is already fully qualified: DB.schema.object.#temp
            return canonical
        except Exception:
            pass  # fallback to tempdb if canonical name fails
    
    return ".".join([p for p in [db, sch, name] if p])


def _get_table_name(self, table_expr: exp.Expression, hint: Optional[str] = None) -> str:
    """Extract table name from expression and qualify with current or default database."""
    database_to_use = self.current_database or self.default_database
    explicit_db = False

    # If table_expr is Schema (INSERT INTO table (cols)), extract the actual table from .this
    if isinstance(table_expr, exp.Schema):
        table_expr = table_expr.this

    # SQL keywords that should never be treated as table names (JOIN keywords, etc.)
    JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}
    
    if isinstance(table_expr, exp.Table):
        # sqlglot drops the leading '#' from temp table identifiers in T-SQL.
        # Detect temps via context:
        #  - catalog == tempdb means it's a temp (restore '#')
        #  - simple name present in temp_registry (as '#name')
        # Also detect table variables (starting with '@') and preserve the '@' prefix
        try:
            simple = str(table_expr.name)
            # CRITICAL: Filter out JOIN keywords that should never be table names
            if simple.lower() in JOIN_KEYWORDS:
                return "unknown"
            # Check if original expression starts with @ (table variable)
            # Table variables should be skipped from lineage (not materialized outputs)
            if str(table_expr).startswith('@'):
                return f"@{simple}"
            if getattr(table_expr, 'catalog', None):
                cat = str(table_expr.catalog)
                if cat and cat.lower() == 'tempdb':
                    # Use canonical temp name instead of tempdb..#
                    return self._canonical_temp_name(f"#{simple}")
            # If we have materialized this temp earlier in the procedure, map it to canonical form
            if simple and (f"#{simple}" in self.temp_registry):
                return self._canonical_temp_name(f"#{simple}")
        except Exception:
            pass
        catalog = str(table_expr.catalog) if table_expr.catalog else None
        if catalog and catalog.lower() in {"view", "function", "procedure"}:
            catalog = None
        if catalog and table_expr.db:
            # Explicit DB qualifier in SQL – trust it and do not override via registry.
            full_name = f"{catalog}.{table_expr.db}.{table_expr.name}"
            explicit_db = True
        elif table_expr.db:
            table_name = f"{table_expr.db}.{table_expr.name}"
            full_name = qualify_identifier(table_name, database_to_use)
        else:
            table_name = str(table_expr.name)
            # Check if this is a CTE reference - CTEs should not be qualified with database
            if table_name in self.cte_registry:
                # Return CTE name as-is without qualification
                return table_name
            full_name = qualify_identifier(table_name, database_to_use)
    elif isinstance(table_expr, exp.Identifier):
        # Identifiers may also point at temps without leading '#'. If present in temp_registry, use canonical name.
        try:
            ident = str(table_expr.this)
            if ident and (f"#{ident}" in self.temp_registry):
                return self._canonical_temp_name(f"#{ident}")
            # Check if this is a CTE reference
            if ident in self.cte_registry:
                return ident
        except Exception:
            pass
        table_name = str(table_expr.this)
        full_name = qualify_identifier(table_name, database_to_use)
    else:
        full_name = hint or "unknown"

    # Temp tables: use canonical naming instead of tempdb..#
    if full_name and (full_name.startswith('#') or full_name.lower().startswith('tempdb..#')):
        temp_name = full_name.lstrip('#') if full_name.startswith('#') else full_name.split('..#')[-1]
        return self._canonical_temp_name(f"#{temp_name}")

    # Registry-aware DB resolution:
    # If the DB came from a weak default (InfoTrackerDW/InfoTrackerDB),
    # prefer a learned mapping from ObjectDbRegistry when available.
    try:
        if not explicit_db and full_name and getattr(self, "registry", None):
            parts = [p for p in (full_name or "").split(".") if p != ""]
            if len(parts) >= 2:
                schema_table = ".".join(parts[-2:])
                # Current DB context inferred from identifier or parser defaults
                current_db = parts[0] if len(parts) >= 3 else database_to_use
                weak_defaults = {"infotrackerdb", "infotrackerdw"}
                if current_db and str(current_db).lower() in weak_defaults:
                    resolved_db = self.registry.resolve("table", schema_table, fallback=current_db)
                    if resolved_db and str(resolved_db).upper() != str(current_db).upper():
                        full_name = ".".join([str(resolved_db).upper(), parts[-2], parts[-1]])
    except Exception:
        # On any registry or resolution error, fall back to original full_name
        pass

    return sanitize_name(full_name)


def _get_full_table_name(self, table_name: str) -> str:
    """Get full table name with database prefix using current or default database.
    Rules:
    - name -> db.dbo.name
    - schema.table -> db.schema.table
    - db.schema.table -> as-is
    """
    # SQL keywords that should never be treated as table names
    JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}
    
    # Check if table_name is a SQL keyword - return as-is to prevent qualification
    parts_check = (table_name or "").split('.')
    if parts_check and parts_check[-1].lower() in JOIN_KEYWORDS:
        # Don't qualify keywords - return original to signal it's not a real table
        return table_name
    
    # Check if this is a CTE reference - CTEs should not be qualified
    simple_name = parts_check[-1] if parts_check else table_name
    if simple_name and simple_name in self.cte_registry:
        # Return CTE name as-is without qualification
        return simple_name
    
    db_to_use = self.current_database or self.default_database or "InfoTrackerDW"
    parts = (table_name or "").split('.')
    parts = [p for p in parts if p != ""]

    # If no DB segment is present and we're on a weak default DB, consult the registry.
    # This allows a learned EDW/STG mapping to override generic InfoTrackerDW.
    try:
        if len(parts) <= 2 and getattr(self, "registry", None):
            weak_defaults = {"infotrackerdb", "infotrackerdw"}
            if db_to_use and str(db_to_use).lower() in weak_defaults:
                if len(parts) == 2:
                    schema_table = ".".join(parts)
                elif len(parts) == 1 and parts[0]:
                    schema_table = f"dbo.{parts[0]}"
                else:
                    schema_table = None
                if schema_table:
                    resolved_db = self.registry.resolve("table", schema_table, fallback=db_to_use)
                    if resolved_db:
                        db_to_use = resolved_db
    except Exception:
        # On any registry or resolution error, keep original db_to_use
        pass

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

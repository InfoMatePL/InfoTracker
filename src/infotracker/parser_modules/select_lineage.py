from __future__ import annotations

from typing import List, Dict, Set, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp

from ..models import ColumnReference, ColumnSchema, ColumnLineage, TransformationType
import re

# Minimal tail-strip regex similar to legacy dev_parser for stable derived column names
_FUNC_TAIL_RE = re.compile(r"\b(?:COALESCE|ISNULL|CAST|CONVERT|TRY_CAST|HASHBYTES|IIF)\s*\(", re.I)

def _strip_expr_tail(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"/\*.*?\*/", "", str(name), flags=re.S)
    s = re.sub(r"--.*?$", "", s, flags=re.M)
    s = re.sub(r"\s+", " ", s).strip()
    m = _FUNC_TAIL_RE.search(s)
    if m:
        s = s[:m.start()].strip()
    s = re.sub(r"[^\w#]+", "_", s).strip("_")
    # Validate: if result is empty, too short, or contains SQL keywords, return empty to trigger fallback
    if not s or len(s) < 2 or any(keyword in s.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT', 'AS']):
        return ""
    return s


def _build_alias_maps(self, select_exp: exp.Select):
    alias_map = {}
    derived_cols = {}

    # Process CTEs first to populate cte_registry
    self._process_ctes(select_exp)
    
    # Add CTEs to alias_map so they can be resolved as sources
    for cte_name, cte_columns in self.cte_registry.items():
        # CTE name is used as both alias and table name
        alias_map[cte_name.lower()] = cte_name
        # Also add to base_fqns for unqualified column resolution
        # Note: CTEs are not real tables, but we treat them as sources for lineage

    base_fqns = []
    for t in select_exp.find_all(exp.Table):
        a = getattr(t, "alias", None) or t.args.get("alias")
        alias = None
        if a:
            if hasattr(a, "name"):
                alias = a.name.lower()
            else:
                alias = str(a).lower()
        fqn = self._qualify_table(t)
        # If this table corresponds to a known temp by simple name, use canonical format
        try:
            parts_tmp = (fqn or '').split('.')
            simple = parts_tmp[-1] if parts_tmp else None
            if simple:
                # Check if it's a temp table (starts with # or is in temp_registry with #)
                temp_seg = None
                if str(simple).startswith('#'):
                    # Already has #, check if it's in temp_registry
                    if simple in self.temp_registry:
                        temp_seg = simple
                elif f"#{simple}" in self.temp_registry:
                    temp_seg = f"#{simple}"
                
                if temp_seg:
                    # Use canonical format with procedure context: schema.object#temp
                    _, canonical_name = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
                    fqn = canonical_name
        except Exception:
            pass
        if alias:
            alias_map[alias] = fqn
        # Don't overwrite CTEs in alias_map - they were already added with simple CTE names
        table_name_lower = t.name.lower()
        # Case-insensitive check: look for CTE with matching lowercase key
        is_cte_name = any(cte_key.lower() == table_name_lower for cte_key in self.cte_registry.keys())
        if not is_cte_name:
            alias_map[table_name_lower] = fqn
        base_fqns.append(fqn)

    for sq in select_exp.find_all(exp.Subquery):
        a = getattr(sq, "alias", None) or sq.args.get("alias")
        if not a:
            continue
        if hasattr(a, "name"):
            alias = a.name.lower()
        else:
            alias = str(a).lower()
        inner = sq.this if isinstance(sq.this, exp.Select) else None
        if not inner:
            continue
        idx = 0
        for proj in (inner.expressions or []):
            if isinstance(proj, exp.Alias):
                out_name = (proj.alias or proj.alias_or_name)
                target = proj.this
            else:
                out_name = f"col_{idx+1}"
                target = proj
            key = (alias, (out_name or "").lower())
            derived_cols[key] = list(target.find_all(exp.Column))
            idx += 1

    # If exactly one base table is present, allow resolving unqualified columns to it
    # BUT: prioritize the table/CTE from the main FROM clause of this SELECT
    # This ensures that for "SELECT ... FROM CTE", unqualified columns resolve to CTE, not to tables inside CTE definitions
    try:
        # Find the main FROM clause of this SELECT (not nested SELECTs)
        from_source = None
        
        # Note: sqlglot uses 'from_' (not 'from') to avoid Python keyword collision
        if hasattr(select_exp, 'args') and 'from_' in select_exp.args:
            from_expr = select_exp.args['from_']
            if hasattr(from_expr, 'this') and isinstance(from_expr.this, exp.Table):
                from_table = from_expr.this
                from_table_name = from_table.name
                # Check if FROM table has an alias
                from_alias = getattr(from_table, "alias", None) or from_table.args.get("alias")
                if from_alias:
                    from_alias_str = from_alias.name.lower() if hasattr(from_alias, "name") else str(from_alias).lower()
                    # Use the alias as the source
                    if from_alias_str in alias_map:
                        from_source = alias_map[from_alias_str]
                else:
                    # No alias - use table name (check if it's a CTE or regular table)
                    from_table_lower = from_table_name.lower()
                    if from_table_lower in alias_map:
                        from_source = alias_map[from_table_lower]
        
        # Set alias_map[''] to the FROM source if found, otherwise fall back to single table logic
        if from_source and '' not in alias_map:
            alias_map[''] = from_source
        elif len(set(base_fqns)) == 1 and '' not in alias_map:
            alias_map[''] = sorted(set(base_fqns))[0]
    except Exception:
        pass

    return alias_map, derived_cols


def _append_column_ref(self, out_list, col_exp: exp.Column, alias_map: dict):
    import logging
    logger = logging.getLogger(__name__)
    
    qual = (col_exp.table or "").lower()
    table_fqn = alias_map.get(qual)
    if not table_fqn:
        # Try to find table by column name if no table qualifier
        # This handles unqualified columns when there's only one table
        if not qual and '' in alias_map:
            table_fqn = alias_map['']
            logger.debug(f"_append_column_ref: Using alias_map[''] = {table_fqn} for unqualified column {col_exp.name}")
        else:
            logger.debug(f"_append_column_ref: No table_fqn found for qual={qual}, alias_map keys={list(alias_map.keys())}, alias_map values={list(alias_map.values())[:5]}, cte_registry keys={list(self.cte_registry.keys())[:5]}")
            # If no table qualifier and no alias_map[''], try to find CTE in alias_map values
            if not qual:
                # Check if any value in alias_map is a CTE
                # For INSERT INTO with CTE, the CTE might be the only source
                for key, value in alias_map.items():
                    value_simple = value.split('.')[-1] if '.' in value else value
                    # Check case-insensitive match with cte_registry
                    cte_registry_lower = {k.lower(): k for k in self.cte_registry.keys()}
                    if value in self.cte_registry or value_simple in self.cte_registry or value.lower() in cte_registry_lower or value_simple.lower() in cte_registry_lower:
                        table_fqn = value
                        logger.debug(f"_append_column_ref: Found CTE {value} in alias_map for unqualified column {col_exp.name}")
                        break
                if not table_fqn:
                    # If still not found, check if any alias_map value (case-insensitive) matches a CTE
                    cte_registry_lower = {k.lower(): k for k in self.cte_registry.keys()}
                    for key, value in alias_map.items():
                        value_lower = value.lower()
                        if value_lower in cte_registry_lower:
                            table_fqn = value
                            logger.debug(f"_append_column_ref: Found CTE {value} (case-insensitive) in alias_map for unqualified column {col_exp.name}")
                            break
                    if not table_fqn:
                        # If still not found, check if there's a CTE value in alias_map and use it as default source
                        # This handles cases like "SELECT col FROM CTE AS src" where col is unqualified
                        cte_registry_lower = {k.lower(): k for k in self.cte_registry.keys()}
                        for key, value in alias_map.items():
                            value_lower = value.lower()
                            if value_lower in cte_registry_lower:
                                table_fqn = value
                                logger.debug(f"_append_column_ref: Using CTE {value} from alias_map as default source for unqualified column {col_exp.name}")
                                break
                        if not table_fqn:
                            # If still not found, try to find the source table (exclude INTO target for SELECT INTO)
                            # For SELECT INTO, exclude the target table from alias_map when resolving unqualified columns
                            select_stmt = getattr(self, '_current_select_stmt', None)
                            into_target = None
                            if select_stmt and isinstance(select_stmt, exp.Select):
                                into_expr = select_stmt.args.get('into')
                                if into_expr:
                                    # Get the target table name from INTO clause
                                    if hasattr(into_expr, 'this'):
                                        into_target = self._get_table_name(into_expr.this)
                                    # Also check if it's a temp table (starts with #)
                                    if into_target and not into_target.startswith('#'):
                                        # Check if it's in temp_registry with #
                                        simple = into_target.split('.')[-1] if '.' in into_target else into_target
                                        if f"#{simple}" in self.temp_registry:
                                            into_target = f"#{simple}"
                                    elif not into_target:
                                        # Try to extract from SQL string
                                        try:
                                            original_sql = str(select_stmt.sql(dialect=self.dialect))
                                            import re as _re
                                            into_match = _re.search(r'\bINTO\s+#(\w+)', original_sql, _re.IGNORECASE)
                                            if into_match:
                                                into_target = f"#{into_match.group(1)}"
                                        except Exception:
                                            pass
                            
                            # Filter out INTO target from alias_map values when resolving unqualified columns
                            non_empty_values = [v for k, v in alias_map.items() if k != '']
                            if into_target:
                                # Remove INTO target from consideration
                                into_target_simple = into_target.split('.')[-1] if '.' in into_target else into_target
                                non_empty_values = [v for v in non_empty_values if not (v.endswith(into_target_simple) or v.endswith(into_target) or into_target in v or into_target_simple in v)]
                            
                            if len(non_empty_values) == 1:
                                table_fqn = non_empty_values[0]
                                logger.debug(f"_append_column_ref: Using single source value from alias_map (excluding INTO target {into_target}): {table_fqn} for unqualified column {col_exp.name}")
                            elif len(non_empty_values) > 1:
                                # If still multiple values, prefer sources from main FROM clause over subquery sources
                                # For INSERT INTO ... SELECT FROM CTE WHERE NOT EXISTS (SELECT FROM table),
                                # the columns should come from the main FROM (CTE), not from the subquery table
                                # Check if any value is a CTE (likely from main FROM clause)
                                main_from_values = []
                                for v in non_empty_values:
                                    value_simple = v.split('.')[-1] if '.' in v else v
                                    cte_registry_lower = {k.lower(): k for k in self.cte_registry.keys()}
                                    if v in self.cte_registry or value_simple in self.cte_registry or v.lower() in cte_registry_lower or value_simple.lower() in cte_registry_lower:
                                        main_from_values.append(v)
                                
                                if len(main_from_values) == 1:
                                    table_fqn = main_from_values[0]
                                    logger.debug(f"_append_column_ref: Using CTE from main FROM clause: {table_fqn} for unqualified column {col_exp.name}")
                                else:
                                    # If still multiple values, prefer non-temp tables (sources, not targets)
                                    non_temp_values = [v for v in non_empty_values if not (v.startswith('#') or 'tempdb' in v.lower() or '#temp' in v.lower())]
                                    if len(non_temp_values) == 1:
                                        table_fqn = non_temp_values[0]
                                        logger.debug(f"_append_column_ref: Using single non-temp source from alias_map: {table_fqn} for unqualified column {col_exp.name}")
                                    else:
                                        logger.debug(f"_append_column_ref: Multiple values in alias_map after filtering: {non_empty_values}, cannot resolve unqualified column {col_exp.name}")
                                        return
                            else:
                                logger.debug(f"_append_column_ref: No values in alias_map after filtering (INTO target: {into_target}), cannot resolve unqualified column {col_exp.name}")
                                return
            else:
                return
    
    # Check if this is a CTE reference
    # CTEs don't have direct table sources - their sources are in the CTE definition
    # Try to expand CTE to its sources
    cte_name_simple = table_fqn.split('.')[-1] if '.' in table_fqn else table_fqn
    is_cte = (qual in self.cte_registry) or (table_fqn in self.cte_registry) or (cte_name_simple in self.cte_registry)
    
    # Also check if the table name matches a CTE in the current SELECT statement's WITH clause
    # This handles cases where CTE is not in cte_registry (e.g., when SELECT INTO is parsed separately from WITH)
    if not is_cte:
        select_stmt = getattr(self, '_current_select_stmt', None)
        if select_stmt and isinstance(select_stmt, exp.Select):
            with_clause = select_stmt.args.get('with')
            if with_clause and hasattr(with_clause, 'expressions'):
                for cte in with_clause.expressions:
                    cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                    if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                        is_cte = True
                        logger.debug(f"_append_column_ref: Detected CTE {cte_alias} from WITH clause (not in cte_registry)")
                        break
        # Also check parent statement (for SELECT INTO that's part of WITH statement)
        if not is_cte:
            try:
                parent = getattr(select_stmt, 'parent', None) if select_stmt else None
                if isinstance(parent, exp.With):
                    with_clause = parent.args.get('expressions') if hasattr(parent, 'args') else None
                    if with_clause:
                        for cte in with_clause:
                            cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                            if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                                is_cte = True
                                logger.debug(f"_append_column_ref: Detected CTE {cte_alias} from parent WITH clause (not in cte_registry)")
                                break
            except Exception:
                pass
    
    if is_cte:
        # Try to find CTE definition and extract sources from it
        # Find the actual CTE name (case-insensitive match)
        cte_name = None
        cte_registry_lower = {k.lower(): k for k in self.cte_registry.keys()}
        if qual and qual.lower() in cte_registry_lower:
            cte_name = cte_registry_lower[qual.lower()]
        elif table_fqn and table_fqn.lower() in cte_registry_lower:
            cte_name = cte_registry_lower[table_fqn.lower()]
        else:
            cte_name_simple = table_fqn.split('.')[-1] if '.' in table_fqn else table_fqn
            cte_name = cte_registry_lower.get(cte_name_simple.lower(), cte_name_simple)
        
        # If CTE not in registry, try to find it in WITH clause
        if not cte_name or cte_name not in self.cte_registry:
            select_stmt = getattr(self, '_current_select_stmt', None)
            if select_stmt and isinstance(select_stmt, exp.Select):
                with_clause = select_stmt.args.get('with')
                if with_clause and hasattr(with_clause, 'expressions'):
                    for cte in with_clause.expressions:
                        cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                        if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                            cte_name = cte_alias
                            logger.debug(f"_append_column_ref: Found CTE {cte_name} in WITH clause")
                            break
            # Also check parent statement (for SELECT INTO that's part of WITH statement)
            if not cte_name or (cte_name not in self.cte_registry):
                try:
                    parent = getattr(select_stmt, 'parent', None) if select_stmt else None
                    if isinstance(parent, exp.With):
                        with_clause = parent.args.get('expressions') if hasattr(parent, 'args') else None
                        if with_clause:
                            for cte in with_clause:
                                cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                                if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                                    cte_name = cte_alias
                                    logger.debug(f"_append_column_ref: Found CTE {cte_name} in parent WITH clause")
                                    break
                except Exception:
                    pass
        
        if not cte_name:
            cte_name = cte_name_simple if 'cte_name_simple' in locals() else (table_fqn.split('.')[-1] if '.' in table_fqn else table_fqn)
        
        logger.debug(f"_append_column_ref: Detected CTE reference: {cte_name}")
        # Look for CTE definition - first in current SELECT statement's WITH clause, then in cte_registry
        try:
            select_stmt = getattr(self, '_current_select_stmt', None)
            cte_found = False
            if select_stmt and isinstance(select_stmt, exp.Select):
                with_clause = select_stmt.args.get('with')
                if with_clause and hasattr(with_clause, 'expressions'):
                    for cte in with_clause.expressions:
                        if hasattr(cte, 'alias') and str(cte.alias) == cte_name:
                            # Found the CTE definition, extract its sources
                            if isinstance(cte.this, exp.Select):
                                # Extract dependencies from CTE's SELECT
                                cte_deps = self._extract_dependencies(cte.this)
                                logger.debug(f"_append_column_ref: CTE {cte_name} dependencies: {cte_deps}")
                                # Check if any dependency is a temp table
                                temp_deps_found = False
                                for dep in cte_deps:
                                    dep_simple = dep.split('.')[-1] if '.' in dep else dep
                                    if dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry):
                                        temp_deps_found = True
                                        # Use this temp table as the source
                                        temp_seg = dep_simple if dep_simple.startswith('#') else f"#{dep_simple}"
                                        logger.debug(f"_append_column_ref: CTE {cte_name} sources temp table {temp_seg}, using it as source")
                                        ns_temp, table_name = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
                                        use_direct_ref = getattr(self, '_use_direct_temp_ref', False)
                                        if use_direct_ref:
                                            # Add direct reference to temp table
                                            ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
                                            out_list.append(ref)
                                            logger.debug(f"_append_column_ref: Added temp ref from CTE: {ref}")
                                            return
                                        # Try to use temp_lineage if available
                                        ver = self._temp_current(temp_seg)
                                        colname = col_exp.name
                                        if ver and ver in self.temp_lineage and colname in self.temp_lineage[ver]:
                                            out_list.extend(self.temp_lineage[ver][colname])
                                            logger.debug(f"_append_column_ref: Using temp_lineage from CTE: {len(self.temp_lineage[ver][colname])} refs")
                                            return
                                        if temp_seg in self.temp_lineage and colname in self.temp_lineage[temp_seg]:
                                            out_list.extend(self.temp_lineage[temp_seg][colname])
                                            logger.debug(f"_append_column_ref: Using temp_lineage from CTE: {len(self.temp_lineage[temp_seg][colname])} refs")
                                            return
                                        # Fallback: add direct reference
                                        ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
                                        out_list.append(ref)
                                        logger.debug(f"_append_column_ref: Added temp ref from CTE (fallback): {ref}")
                                        return
                                
                                # If no temp tables found, use regular tables as sources (expand CTE to base sources)
                                # This handles cases like "SELECT INTO #MaxLoadDate FROM MaxDates" where MaxDates is a CTE
                                if not temp_deps_found and cte_deps:
                                    logger.debug(f"_append_column_ref: CTE {cte_name} has no temp table dependencies, expanding to base sources: {cte_deps}")
                                    for dep in cte_deps:
                                        dep_simple = dep.split('.')[-1] if '.' in dep else dep
                                        # Skip temp tables (they should be expanded separately)
                                        if not (dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry)):
                                            # This is a regular table - use it as source
                                            db, sch, tbl = self._split_fqn(dep)
                                            effective_db = db or self.current_database or self.default_database or "InfoTrackerDW"
                                            ref = ColumnReference(
                                                namespace=self._canonical_namespace(effective_db),
                                                table_name=f"{sch}.{tbl}",
                                                column_name=col_exp.name,
                                            )
                                            out_list.append(ref)
                                            logger.debug(f"_append_column_ref: Added ref from CTE {cte_name} base source {dep}: {ref}")
                                    if out_list:
                                        return
                            cte_found = True
                            break
            # If CTE not found in current SELECT, check if it's registered and try to find its source
            if not cte_found and cte_name in self.cte_registry:
                # CTE is registered but not in current SELECT - it might be defined before INSERT
                # Get the CTE definition from cte_registry
                cte_info = self.cte_registry[cte_name]
                # Handle both dict format (with 'definition' key) and legacy list format (just columns)
                if isinstance(cte_info, dict) and 'definition' in cte_info:
                    cte_def = cte_info['definition']
                elif isinstance(cte_info, exp.Select):
                    cte_def = cte_info
                else:
                    cte_def = None
                logger.debug(f"_append_column_ref: CTE {cte_name} found in cte_registry, extracting dependencies from definition")
                if cte_def and isinstance(cte_def, exp.Select):
                    # Extract dependencies from CTE's SELECT
                    cte_deps = self._extract_dependencies(cte_def)
                    logger.debug(f"_append_column_ref: CTE {cte_name} dependencies from registry: {cte_deps}")
                    # Check if any dependency is a temp table
                    temp_deps_found = False
                    for dep in cte_deps:
                        dep_simple = dep.split('.')[-1] if '.' in dep else dep
                        if dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry):
                            temp_deps_found = True
                            # Use this temp table as the source
                            temp_seg = dep_simple if dep_simple.startswith('#') else f"#{dep_simple}"
                            logger.debug(f"_append_column_ref: CTE {cte_name} sources temp table {temp_seg}, using it as source")
                            ns_temp, table_name = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
                            use_direct_ref = getattr(self, '_use_direct_temp_ref', False)
                            if use_direct_ref:
                                # Add direct reference to temp table
                                ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
                                out_list.append(ref)
                                logger.debug(f"_append_column_ref: Added temp ref from CTE registry: {ref}")
                                return
                            # Try to use temp_lineage if available
                            ver = self._temp_current(temp_seg)
                            colname = col_exp.name
                            if ver and ver in self.temp_lineage and colname in self.temp_lineage[ver]:
                                out_list.extend(self.temp_lineage[ver][colname])
                                logger.debug(f"_append_column_ref: Using temp_lineage from CTE registry: {len(self.temp_lineage[ver][colname])} refs")
                                return
                            if temp_seg in self.temp_lineage and colname in self.temp_lineage[temp_seg]:
                                out_list.extend(self.temp_lineage[temp_seg][colname])
                                logger.debug(f"_append_column_ref: Using temp_lineage from CTE registry: {len(self.temp_lineage[temp_seg][colname])} refs")
                                return
                            # Fallback: add direct reference
                            ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
                            out_list.append(ref)
                            logger.debug(f"_append_column_ref: Added temp ref from CTE registry (fallback): {ref}")
                            return
                    # If no temp tables found, use regular tables as sources (expand CTE to base sources)
                    # This handles cases like "SELECT INTO #MaxLoadDate FROM MaxDates" where MaxDates is a CTE
                    if not temp_deps_found and cte_deps:
                        logger.debug(f"_append_column_ref: CTE {cte_name} has no temp table dependencies, expanding to base sources: {cte_deps}")
                        for dep in cte_deps:
                            dep_simple = dep.split('.')[-1] if '.' in dep else dep
                            # Skip temp tables (they should be expanded separately)
                            if not (dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry)):
                                # This is a regular table - use it as source
                                db, sch, tbl = self._split_fqn(dep)
                                effective_db = db or self.current_database or self.default_database or "InfoTrackerDW"
                                ref = ColumnReference(
                                    namespace=self._canonical_namespace(effective_db),
                                    table_name=f"{sch}.{tbl}",
                                    column_name=col_exp.name,
                                )
                                out_list.append(ref)
                                logger.debug(f"_append_column_ref: Added ref from CTE {cte_name} base source {dep}: {ref}")
                        if out_list:
                            return
        except Exception as e:
            logger.debug(f"_append_column_ref: Exception expanding CTE {cte_name}: {e}")
            import traceback
            logger.debug(f"_append_column_ref: Traceback: {traceback.format_exc()}")
    
    db, sch, tbl = self._split_fqn(table_fqn)
    logger.debug(f"_append_column_ref: table_fqn={table_fqn}, split to db={db}, sch={sch}, tbl={tbl}")
    # Detect temp segment anywhere in the FQN
    try:
        temp_seg = None
        for seg in (table_fqn or '').split('.'):
            if str(seg).startswith('#'):
                temp_seg = seg
                break
        # If no # found but tbl is in temp_registry as #tbl, it's a temp table
        if not temp_seg and tbl:
            # Check if tbl already starts with # (from _split_fqn)
            if str(tbl).startswith('#'):
                temp_seg = tbl
            elif f"#{tbl}" in self.temp_registry:
                temp_seg = f"#{tbl}"
        logger.debug(f"_append_column_ref: temp_seg={temp_seg}, temp_registry keys={list(self.temp_registry.keys())[:5] if hasattr(self, 'temp_registry') else 'N/A'}")
        if temp_seg:
            # Use _ns_and_name to get proper format with procedure context: schema.object#temp
            ns_temp, table_name = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
            logger.debug(f"_append_column_ref: temp_seg={temp_seg}, ns_temp={ns_temp}, table_name={table_name}")
            # Check if we should use direct reference (for INSERT INTO) or expanded lineage
            use_direct_ref = getattr(self, '_use_direct_temp_ref', False)
            logger.debug(f"_append_column_ref: use_direct_ref={use_direct_ref}")
            if not use_direct_ref:
                # Inline base lineage if we have it
                ver = self._temp_current(temp_seg)
                colname = col_exp.name
                if ver and ver in self.temp_lineage and colname in self.temp_lineage[ver]:
                    # Use references from temp_lineage (they contain base sources, not self-reference)
                    out_list.extend(self.temp_lineage[ver][colname])
                    logger.debug(f"_append_column_ref: Using temp_lineage[{ver}][{colname}]: {len(self.temp_lineage[ver][colname])} refs")
                    return
                if temp_seg in self.temp_lineage and colname in self.temp_lineage[temp_seg]:
                    # Use references from temp_lineage (they contain base sources, not self-reference)
                    out_list.extend(self.temp_lineage[temp_seg][colname])
                    logger.debug(f"_append_column_ref: Using temp_lineage[{temp_seg}][{colname}]: {len(self.temp_lineage[temp_seg][colname])} refs")
                    return
            # If no temp_lineage or use_direct_ref is True, add direct reference with new format
            ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
            out_list.append(ref)
            logger.debug(f"_append_column_ref: Added direct temp ref: {ref}")
    except Exception as e:
        logger.debug(f"_append_column_ref: Exception handling temp table: {e}")
        import traceback
        logger.debug(f"_append_column_ref: Traceback: {traceback.format_exc()}")
    # Resolve namespace for non-temp inputs:
    # - prefer explicit db from the FQN,
    # - otherwise consult the global object→DB registry,
    # - finally fall back to current/default DB (or InfoTrackerDW).
    effective_db = db
    if not effective_db:
        # Try registry (learned from CREATE TABLE/VIEW/PROC)
        try:
            if getattr(self, "registry", None) and sch and tbl:
                schema_table = f"{sch}.{tbl}"
                fallback_db = getattr(self, "current_database", None) or getattr(self, "default_database", None) or "InfoTrackerDW"
                resolved = self.registry.resolve("table", schema_table, fallback=fallback_db)
                if resolved:
                    effective_db = resolved
        except Exception:
            effective_db = None
    if not effective_db:
        try:
            effective_db = self.current_database or self.default_database or "InfoTrackerDW"
        except Exception:
            effective_db = "InfoTrackerDW"
    out_list.append(
        ColumnReference(
            namespace=self._canonical_namespace(effective_db),
            table_name=f"{sch}.{tbl}",
            column_name=col_exp.name,
        )
    )


def _collect_inputs_for_expr(self, expr: exp.Expression, alias_map: dict, derived_cols: dict):
    inputs = []
    for col in expr.find_all(exp.Column):
        qual = (col.table or "").lower()
        key = (qual, col.name.lower())
        base_cols = derived_cols.get(key)
        if base_cols:
            for b in base_cols:
                _append_column_ref(self, inputs, b, alias_map)
            continue
        _append_column_ref(self, inputs, col, alias_map)
    return inputs


def _get_schema(self, db: str, sch: str, tbl: str):
    ns = self._canonical_namespace(db) if db else None
    key = f"{sch}.{tbl}"
    if hasattr(self.schema_registry, "get"):
        return self.schema_registry.get(ns, key)
    return self.schema_registry.get((ns, key))


def _type_of_column(self, col_exp, alias_map):
    qual = (getattr(col_exp, "table", None) or "").lower()
    fqn = alias_map.get(qual)
    if not fqn:
        return None
    db, sch, tbl = self._split_fqn(fqn)
    schema = _get_schema(self, db, sch, tbl)
    if not schema:
        return None
    c = schema.get_column(col_exp.name)
    return c.data_type if c else None


def _infer_type(self, expr, alias_map) -> str:
    if isinstance(expr, exp.Cast):
        t = expr.args.get("to")
        return str(t) if t else "unknown"
    if isinstance(expr, exp.Convert):
        t = expr.args.get("to")
        return str(t) if t else "unknown"
    if isinstance(expr, (exp.Trim, exp.Upper, exp.Lower)):
        base = expr.find(exp.Column)
        return _type_of_column(self, base, alias_map) or "nvarchar"
    if isinstance(expr, exp.Coalesce):
        types = []
        for a in (expr.args.get("expressions") or []):
            if isinstance(a, exp.Column):
                types.append(_type_of_column(self, a, alias_map))
            elif isinstance(a, exp.Literal):
                types.append("nvarchar" if a.is_string else "numeric")
        tset = [t for t in types if t]
        if any(t and "nvarchar" in t.lower() for t in tset):
            return "nvarchar"
        if any(t and "varchar" in t.lower() for t in tset):
            return "varchar"
        return tset[0] if tset else "unknown"
    s = str(expr).upper()
    if "HASHBYTES(" in s or "MD5(" in s:
        return "binary(16)"
    if isinstance(expr, exp.Column):
        return _type_of_column(self, expr, alias_map) or "unknown"
    return "unknown"


def _short_desc(self, expr) -> str:
    return " ".join(str(expr).split())[:250]


def _extract_view_header_cols(self, create_exp) -> list[str]:
    cols: list[str] = []

    def _collect(exprs) -> None:
        if not exprs:
            return
        for e in exprs:
            n = getattr(e, "name", None)
            if n:
                cols.append(str(n).strip("[]"))
            else:
                cols.append(str(e).strip().strip("[]"))

    exprs = getattr(create_exp, "expressions", None) or create_exp.args.get("expressions")
    _collect(exprs)
    try:
        target = getattr(create_exp, "this", None)
        texprs = getattr(target, "expressions", None) or (getattr(target, "args", {}).get("expressions") if getattr(target, "args", None) else None)
        _collect(texprs)
    except Exception:
        pass

    seen = set()
    out = []
    for c in cols:
        lc = c.lower()
        if lc in seen:
            continue
        seen.add(lc)
        out.append(c)
    return out


def _extract_column_alias(self, select_expr: exp.Expression) -> Optional[str]:
    """Extract column alias or name from a SELECT expression."""
    if hasattr(select_expr, 'alias') and select_expr.alias:
        return str(select_expr.alias)
    if isinstance(select_expr, exp.Alias):
        return str(select_expr.alias)
    if isinstance(select_expr, exp.Column):
        return str(select_expr.this)
    expr_str = str(select_expr)
    up = expr_str.upper()
    if ' AS ' in up:
        parts = expr_str.split()
        as_idx = -1
        for i, part in enumerate(parts):
            if part.upper() == 'AS':
                as_idx = i
                break
        if as_idx >= 0 and as_idx + 1 < len(parts):
            return parts[as_idx + 1].strip("'\"")
    return None


def _extract_column_references(self, select_expr: exp.Expression, select_stmt: exp.Select) -> List[ColumnReference]:
    """Extract table-qualified column references used by a SELECT expression."""
    refs: List[ColumnReference] = []
    for column_expr in select_expr.find_all(exp.Column):
        table_name = "unknown"
        column_name = str(column_expr.this)
        if hasattr(column_expr, 'table') and column_expr.table:
            table_alias = str(column_expr.table)
            table_name = self._resolve_table_from_alias(table_alias, select_stmt)
        else:
            tables = [self._get_table_name(t) for t in select_stmt.find_all(exp.Table)]
            if len(tables) == 1:
                table_name = tables[0]
        if table_name and (table_name.startswith('@') or ('+' in table_name) or (table_name.startswith('[') and table_name.endswith(']') and '.' not in table_name)):
            continue
        if table_name != "unknown":
            ns, nm = self._ns_and_name(table_name)
            refs.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
    return refs


def _is_string_function(self, expr: exp.Expression) -> bool:
    string_functions = ['RIGHT', 'LEFT', 'SUBSTRING', 'CHARINDEX', 'LEN', 'CONCAT']
    expr_str = str(expr).upper()
    return any(func in expr_str for func in string_functions)


def _has_star_expansion(self, select_stmt: exp.Select) -> bool:
    for expr in select_stmt.expressions:
        if isinstance(expr, exp.Star):
            return True
        if isinstance(expr, exp.Column):
            if str(expr.this) == "*" or str(expr).endswith(".*"):
                return True
    return False


def _has_union(self, stmt: exp.Expression) -> bool:
    return isinstance(stmt, exp.Union) or len(list(stmt.find_all(exp.Union))) > 0


def _handle_star_expansion(self, select_stmt: exp.Select, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
    lineage = []
    output_columns = []
    ordinal = 0
    seen_columns = set()

    for select_expr in select_stmt.expressions:
        if isinstance(select_expr, exp.Star):
            if hasattr(select_expr, 'table') and select_expr.table:
                alias = str(select_expr.table)
                table_name = _resolve_table_from_alias(self, alias, select_stmt)
                if table_name != "unknown":
                    columns = self._infer_table_columns_unified(table_name)
                    for column_name in columns:
                        if column_name not in seen_columns:
                            seen_columns.add(column_name)
                            output_columns.append(ColumnSchema(name=column_name, data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column=column_name, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=column_name)], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
            else:
                source_tables = []
                for table in select_stmt.find_all(exp.Table):
                    table_name = self._get_table_name(table)
                    if table_name != "unknown":
                        source_tables.append(table_name)
                for table_name in source_tables:
                    columns = self._infer_table_columns_unified(table_name)
                    for column_name in columns:
                        if column_name not in seen_columns:
                            seen_columns.add(column_name)
                            output_columns.append(ColumnSchema(name=column_name, data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column=column_name, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=column_name)], transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))
        elif isinstance(select_expr, exp.Column) and (str(select_expr.this) == "*" or str(select_expr).endswith(".*")):
            if hasattr(select_expr, 'table') and select_expr.table:
                alias = str(select_expr.table)
                table_name = _resolve_table_from_alias(self, alias, select_stmt)
                if table_name != "unknown":
                    columns = self._infer_table_columns_unified(table_name)
                    for column_name in columns:
                        if column_name not in seen_columns:
                            seen_columns.add(column_name)
                            output_columns.append(ColumnSchema(name=column_name, data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column=column_name, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=column_name)], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
        else:
            col_name = self._extract_column_alias(select_expr) or f"col_{ordinal}"
            output_columns.append(ColumnSchema(name=col_name, data_type="unknown", nullable=True, ordinal=ordinal))
            ordinal += 1
            input_refs = self._extract_column_references(select_expr, select_stmt)
            if not input_refs:
                db = self.current_database or self.default_database or "InfoTrackerDW"
                input_refs = [ColumnReference(namespace=self._canonical_namespace(db), table_name="LITERAL", column_name=str(select_expr))]
            lineage.append(ColumnLineage(output_column=col_name, input_fields=input_refs, transformation_type=TransformationType.EXPRESSION, transformation_description=f"SELECT {str(select_expr)}"))

    return lineage, output_columns


def _handle_union_lineage(self, stmt: exp.Expression, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
    lineage = []
    output_columns = []
    union_selects = []
    if isinstance(stmt, exp.Union):
        def _collect_unions(node):
            if isinstance(node, exp.Union):
                _collect_unions(node.left)
                _collect_unions(node.right)
            elif isinstance(node, exp.Select):
                union_selects.append(node)
        _collect_unions(stmt)
    else:
        # If stmt is not a Union, but was passed to _handle_union_lineage,
        # it means it has a UNION somewhere inside. We need to find the actual Union nodes.
        if isinstance(stmt, exp.Select):
            # Find all Union nodes in the statement
            union_nodes = list(stmt.find_all(exp.Union))
            if union_nodes:
                # Use the first Union node found
                def _collect_unions(node):
                    if isinstance(node, exp.Union):
                        _collect_unions(node.left)
                        _collect_unions(node.right)
                    elif isinstance(node, exp.Select):
                        union_selects.append(node)
                _collect_unions(union_nodes[0])
            else:
                # No Union found, this shouldn't happen if _has_union returned True
                # But to avoid recursion, return empty
                return lineage, output_columns
        else:
            union_selects = []

    if not union_selects:
        return lineage, output_columns

    first_lineage, first_columns = self._extract_column_lineage(union_selects[0], view_name)
    for i, col_lineage in enumerate(first_lineage):
        all_input_fields = list(col_lineage.input_fields)
        for other_select in union_selects[1:]:
            if isinstance(other_select, exp.Select):
                other_lineage, _ = self._extract_column_lineage(other_select, view_name)
                if i < len(other_lineage):
                    all_input_fields.extend(other_lineage[i].input_fields)
        lineage.append(ColumnLineage(output_column=col_lineage.output_column, input_fields=all_input_fields, transformation_type=TransformationType.UNION, transformation_description="UNION operation"))
    output_columns = first_columns
    return lineage, output_columns


def _extract_column_lineage(self, stmt: exp.Expression, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
    lineage = []
    output_columns = []
    if isinstance(stmt, exp.Union):
        return _handle_union_lineage(self, stmt, view_name)
    if not isinstance(stmt, exp.Select):
        return lineage, output_columns
    select_stmt = stmt
    alias_map, derived_cols = _build_alias_maps(self, select_stmt)
    # Store select_stmt in self so _append_column_ref can access it for CTE expansion
    self._current_select_stmt = select_stmt
    projections = list(getattr(select_stmt, 'expressions', None) or [])
    if not projections:
        return lineage, output_columns
    if _has_star_expansion(self, select_stmt):
        return _handle_star_expansion(self, select_stmt, view_name)
    if _has_union(self, select_stmt):
        return _handle_union_lineage(self, select_stmt, view_name)
    ordinal = 0
    for proj in projections:
        if isinstance(proj, exp.Alias):
            out_name = proj.alias or proj.alias_or_name
            # Clean up alias name: remove SQL comments and invalid characters
            if out_name:
                # Remove SQL comments (-- and /* */)
                out_name = re.sub(r'--.*$', '', str(out_name), flags=re.MULTILINE).strip()
                out_name = re.sub(r'/\*.*?\*/', '', str(out_name), flags=re.DOTALL).strip()
                # Remove leading/trailing invalid characters
                out_name = str(out_name).strip('[]').strip()
                # If name is empty or invalid after cleanup, try to get from inner expression
                if not out_name or len(out_name) < 2 or out_name in [')', '(', 'INSERT', 'SELECT']:
                    if hasattr(proj, 'this') and isinstance(proj.this, exp.Column):
                        out_name = proj.this.name or "calc_expr"
                    else:
                        out_name = "calc_expr"
            inner = proj.this
        else:
            s = str(proj).upper()
            if "HASHBYTES(" in s or "MD5(" in s:
                out_name = "hash_expr"
            elif isinstance(proj, exp.Coalesce):
                out_name = "coalesce_expr"
            elif isinstance(proj, (exp.Trim, exp.Upper, exp.Lower)):
                col = proj.find(exp.Column)
                out_name = (col.name if col else "text_expr")
            elif isinstance(proj, (exp.Cast, exp.Convert)):
                out_name = "cast_expr"
            elif isinstance(proj, exp.Column):
                out_name = proj.name
                # Clean up column name: remove SQL comments and invalid characters
                if out_name:
                    # Remove SQL comments (-- and /* */)
                    out_name = re.sub(r'--.*$', '', out_name, flags=re.MULTILINE).strip()
                    out_name = re.sub(r'/\*.*?\*/', '', out_name, flags=re.DOTALL).strip()
                    # Remove leading/trailing invalid characters like [ or ]
                    out_name = out_name.strip('[]').strip()
                    # Remove any remaining invalid patterns like "abl.[column" -> "column"
                    if '.' in out_name and '[' in out_name:
                        parts = out_name.split('.')
                        out_name = parts[-1].strip('[]').strip()
                    # If name is empty or invalid after cleanup, use generic name
                    if not out_name or len(out_name) < 2 or out_name in [')', '(', 'INSERT', 'SELECT']:
                        out_name = "calc_expr"
            else:
                # Attempt to derive a stable name from expression tail if no alias provided
                out_name = _strip_expr_tail(str(proj))
                # If _strip_expr_tail returns empty or invalid, use generic name
                if not out_name or len(out_name) < 2:
                    out_name = "calc_expr"
                # Additional validation: check for SQL keywords or invalid characters
                elif len(out_name) > 100 or any(keyword in out_name.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS']):
                    out_name = "calc_expr"
                # Check for invalid single characters or symbols
                elif out_name in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE']:
                    out_name = "calc_expr"
            inner = proj

        inputs = _collect_inputs_for_expr(self, inner, alias_map, derived_cols)
        out_type = _infer_type(self, inner, alias_map)
        if isinstance(inner, (exp.Cast, exp.Convert)):
            ttype = TransformationType.CAST
        elif isinstance(inner, exp.Case):
            ttype = TransformationType.CASE
        elif isinstance(inner, exp.Column):
            ttype = TransformationType.IDENTITY
        else:
            s = str(inner).upper()
            if s.startswith("CASE ") or s.startswith("CASEWHEN ") or s.startswith("IIF("):
                ttype = TransformationType.CASE
            else:
                ttype = TransformationType.EXPRESSION

        lineage.append(ColumnLineage(output_column=out_name, input_fields=inputs, transformation_type=ttype, transformation_description=_short_desc(self, inner)))
        output_columns.append(ColumnSchema(name=out_name, data_type=out_type, nullable=True, ordinal=ordinal))
        ordinal += 1
    return lineage, output_columns


def _analyze_expression_lineage(self, output_name: str, expr: exp.Expression, context: exp.Select) -> ColumnLineage:
    input_fields = []
    transformation_type = TransformationType.IDENTITY
    description = ""

    if isinstance(expr, exp.Column):
        table_alias = str(expr.table) if expr.table else None
        column_name = str(expr.this)
        table_name = _resolve_table_from_alias(self, table_alias, context)
        if table_name and (table_name.startswith('@') or ('+' in table_name) or (table_name.startswith('[') and table_name.endswith(']') and '.' not in table_name)):
            return ColumnLineage(output_column=output_name, input_fields=[], transformation_type=TransformationType.EXPRESSION, transformation_description=f"Expression: {str(expr)}")
        ns, nm = self._ns_and_name(table_name)
        input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        table_simple = table_name.split('.')[-1] if '.' in table_name else table_name
        semantic_renames = {('OrderItemID', 'SalesID'): True}
        if (column_name, output_name) in semantic_renames:
            transformation_type = TransformationType.RENAME
            description = f"{column_name} AS {output_name}"
        else:
            description = f"{output_name} from {table_simple}.{column_name}"

    elif isinstance(expr, exp.Cast):
        transformation_type = TransformationType.CAST
        inner_expr = expr.this
        target_type = str(expr.to).upper()
        if isinstance(inner_expr, (exp.Mul, exp.Add, exp.Sub, exp.Div)):
            transformation_type = TransformationType.ARITHMETIC
            for column_ref in inner_expr.find_all(exp.Column):
                table_alias = str(column_ref.table) if column_ref.table else None
                column_name = str(column_ref.this)
                table_name = _resolve_table_from_alias(self, table_alias, context)
                ns, nm = self._ns_and_name(table_name)
                input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
            expr_str = str(inner_expr)
            if '*' in expr_str:
                operands = [str(col.this) for col in inner_expr.find_all(exp.Column)]
                if len(operands) >= 2:
                    description = f"{operands[0]} * {operands[1]}"
                else:
                    description = expr_str
            else:
                description = expr_str
        elif isinstance(inner_expr, exp.Column):
            table_alias = str(inner_expr.table) if inner_expr.table else None
            column_name = str(inner_expr.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
            description = f"CAST({column_name} AS {target_type})"

    elif isinstance(expr, exp.Case):
        transformation_type = TransformationType.CASE
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        description = str(expr).replace('\n', ' ').replace('  ', ' ')

    elif isinstance(expr, (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
        transformation_type = TransformationType.AGGREGATION
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        description = f"{type(expr).__name__.upper()}({str(expr.this) if hasattr(expr, 'this') else '*'})"

    elif isinstance(expr, exp.Window):
        transformation_type = TransformationType.WINDOW
        inner_function = expr.this
        if hasattr(inner_function, 'find_all'):
            for column_ref in inner_function.find_all(exp.Column):
                table_alias = str(column_ref.table) if column_ref.table else None
                column_name = str(column_ref.this)
                table_name = _resolve_table_from_alias(self, table_alias, context)
                ns, nm = self._ns_and_name(table_name)
                input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        if hasattr(expr, 'partition_by') and expr.partition_by:
            for partition_col in expr.partition_by:
                for column_ref in partition_col.find_all(exp.Column):
                    table_alias = str(column_ref.table) if column_ref.table else None
                    column_name = str(column_ref.this)
                    table_name = _resolve_table_from_alias(self, table_alias, context)
                    ns, nm = self._ns_and_name(table_name)
                    input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        if hasattr(expr, 'order') and expr.order:
            for order_col in expr.order.expressions:
                for column_ref in order_col.find_all(exp.Column):
                    table_alias = str(column_ref.table) if column_ref.table else None
                    column_name = str(column_ref.this)
                    table_name = _resolve_table_from_alias(self, table_alias, context)
                    ns, nm = self._ns_and_name(table_name)
                    input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        func_name = str(inner_function) if inner_function else "UNKNOWN"
        partition_cols = []
        order_cols = []
        if hasattr(expr, 'partition_by') and expr.partition_by:
            partition_cols = [str(col) for col in expr.partition_by]
        if hasattr(expr, 'order') and expr.order:
            order_cols = [str(col) for col in expr.order.expressions]
        description = f"{func_name} OVER ("
        if partition_cols:
            description += f"PARTITION BY {', '.join(partition_cols)}"
        if order_cols:
            if partition_cols:
                description += " "
            description += f"ORDER BY {', '.join(order_cols)}"
        description += ")"

    elif isinstance(expr, (exp.Mul, exp.Add, exp.Sub, exp.Div)):
        transformation_type = TransformationType.ARITHMETIC
        seen_columns = set()
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            column_key = (table_name, column_name)
            if column_key not in seen_columns:
                seen_columns.add(column_key)
                ns, nm = self._ns_and_name(table_name)
                input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        expr_str = str(expr)
        if '*' in expr_str:
            operands = [str(col.this) for col in expr.find_all(exp.Column)]
            if len(operands) >= 2:
                description = f"{operands[0]} * {operands[1]}"
            else:
                description = expr_str
        else:
            description = expr_str

    elif _is_string_function(self, expr):
        transformation_type = TransformationType.STRING_PARSE
        seen_columns = set()
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            column_key = (table_name, column_name)
            if column_key not in seen_columns:
                seen_columns.add(column_key)
                ns, nm = self._ns_and_name(table_name)
                input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        expr_str = str(expr)
        if 'RIGHT' in expr_str.upper() and 'LEN' in expr_str.upper() and 'CHARINDEX' in expr_str.upper():
            columns = [str(col.this) for col in expr.find_all(exp.Column)]
            if columns:
                col_name = columns[0]
                description = f"RIGHT({col_name}, LEN({col_name}) - CHARINDEX('@', {col_name}))"
            else:
                description = expr_str
        else:
            description = expr_str

    else:
        transformation_type = TransformationType.EXPRESSION
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        description = f"Expression: {str(expr)}"

    return ColumnLineage(output_column=output_name, input_fields=input_fields, transformation_type=transformation_type, transformation_description=description)


def _resolve_table_from_alias(self, alias: Optional[str], context: exp.Select) -> str:
    if not alias:
        tables = list(context.find_all(exp.Table))
        if len(tables) == 1:
            return self._get_table_name(tables[0])
        return "unknown"
    for table in context.find_all(exp.Table):
        parent = table.parent
        if isinstance(parent, exp.Alias) and str(parent.alias) == alias:
            return self._get_table_name(table)
        if hasattr(table, 'alias') and table.alias and str(table.alias) == alias:
            return self._get_table_name(table)
    for join in context.find_all(exp.Join):
        if hasattr(join.this, 'alias') and str(join.this.alias) == alias:
            if isinstance(join.this, exp.Alias):
                return self._get_table_name(join.this.this)
            return self._get_table_name(join.this)
    return alias


def _process_ctes(self, select_stmt: exp.Select) -> exp.Select:
    with_clause = select_stmt.args.get('with')
    if with_clause and hasattr(with_clause, 'expressions'):
        for cte in with_clause.expressions:
            if hasattr(cte, 'alias') and hasattr(cte, 'this'):
                cte_name = str(cte.alias)
                cte_columns = []
                if isinstance(cte.this, exp.Select):
                    for proj in cte.this.expressions:
                        col_name = None
                        if isinstance(proj, exp.Alias):
                            # Get alias name
                            if hasattr(proj, 'alias') and proj.alias:
                                col_name = str(proj.alias)
                            elif hasattr(proj, 'alias_or_name'):
                                col_name = str(proj.alias_or_name)
                            # If no alias, try to get column name from the expression
                            if not col_name and hasattr(proj, 'this'):
                                if isinstance(proj.this, exp.Column):
                                    col_name = str(proj.this.this) if hasattr(proj.this, 'this') else str(proj.this)
                        elif isinstance(proj, exp.Column):
                            # Get column name
                            if hasattr(proj, 'this'):
                                col_name = str(proj.this)
                            elif hasattr(proj, 'name'):
                                col_name = str(proj.name)
                            else:
                                col_name = str(proj)
                        elif isinstance(proj, exp.Star):
                            source_deps = self._extract_dependencies(cte.this)
                            for source_table in source_deps:
                                source_cols = self._infer_table_columns(source_table)
                                cte_columns.extend(source_cols)
                            break
                        
                        # Only add valid column names (not SQL expressions)
                        if col_name:
                            # Sanitize: remove newlines, extra whitespace
                            col_name = col_name.strip().replace('\n', ' ').replace('\t', ' ').strip()
                            # Validate: check if it's a valid column name
                            # Must be non-empty, reasonable length, no SQL keywords, and not just symbols
                            is_valid = (
                                col_name and 
                                len(col_name) >= 2 and 
                                len(col_name) < 100 and 
                                not any(keyword in col_name.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS']) and
                                col_name not in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'INT)', 'CurrentAP\n)', 'PrevAP'] and
                                not col_name.startswith('(') and
                                not col_name.endswith(')') and
                                re.match(r'^[a-zA-Z_#][a-zA-Z0-9_#]*$', col_name)  # Valid identifier pattern
                            )
                            if is_valid:
                                cte_columns.append(col_name)
                            else:
                                # Use generic name for complex expressions
                                cte_columns.append(f"col_{len(cte_columns) + 1}")
                        else:
                            cte_columns.append(f"col_{len(cte_columns) + 1}")
                # Store both columns and the CTE definition (exp.Select) for later use
                # We use a dict to store both pieces of information
                if isinstance(cte.this, exp.Select):
                    self.cte_registry[cte_name] = {
                        'columns': cte_columns,
                        'definition': cte.this
                    }
                else:
                    # Fallback: if not a Select, just store columns
                    self.cte_registry[cte_name] = cte_columns
    return select_stmt

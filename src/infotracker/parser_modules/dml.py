from __future__ import annotations

from typing import Optional, List, Set
from sqlglot import expressions as exp

from ..models import ObjectInfo, TableSchema, ColumnSchema, ColumnLineage, ColumnReference, TransformationType


def _is_placeholder_cols(cols: List[str]) -> bool:
    if not cols:
        return True
    lowered = [str(c).lower() for c in cols if c]
    if not lowered:
        return True
    if len(lowered) == 1 and lowered[0] == "*":
        return True
    return all(c.startswith("unknown_") for c in lowered)


def _should_update_temp_cols(existing: Optional[List[str]], new_cols: List[str]) -> bool:
    if not new_cols:
        return False
    if not existing:
        return True
    existing_lower = [str(c).lower() for c in existing if c]
    new_lower = [str(c).lower() for c in new_cols if c]
    new_placeholder = _is_placeholder_cols(new_cols)
    existing_placeholder = _is_placeholder_cols(existing)
    if existing_placeholder and new_placeholder:
        if new_lower == ["*"] and existing_lower and all(c.startswith("unknown_") for c in existing_lower):
            return True
        if existing_lower == ["*"] and new_lower and all(c.startswith("unknown_") for c in new_lower):
            return False
    if existing_placeholder and not new_placeholder:
        return True
    if not new_placeholder and len(new_cols) > len(existing):
        return True
    if existing_placeholder and new_placeholder and len(new_cols) > len(existing):
        return True
    return False


def _is_select_into(self, statement: exp.Select) -> bool:
    return statement.args.get('into') is not None


def _is_insert_exec(self, statement: exp.Insert) -> bool:
    expression = statement.expression
    return (
        hasattr(expression, 'expressions') and 
        expression.expressions and 
        isinstance(expression.expressions[0], exp.Command) and
        str(expression.expressions[0]).upper().startswith('EXEC')
    )


def _parse_select_into(self, statement: exp.Select, object_hint: Optional[str] = None) -> ObjectInfo:
    import logging
    logger = logging.getLogger(__name__)
    logger.debug(f"[DIAG] _parse_select_into: Called with object_hint={object_hint}")
    
    into_expr = statement.args.get('into')
    if not into_expr:
        raise ValueError("SELECT INTO requires INTO clause")

    # Handle exp.Into - extract the table name from .this
    # SQLGlot parses INTO #table as INTO TEMPORARY table (drops the #)
    # We need to check the original SQL to detect # prefix
    raw_target = None
    is_temp = False
    
    # Check original SQL string for # symbol (SQLGlot drops it)
    try:
        original_sql = str(statement.sql(dialect=self.dialect))
        # Look for INTO #pattern in original SQL
        import re as _re
        into_match = _re.search(r'\bINTO\s+#(\w+)', original_sql, _re.IGNORECASE)
        if into_match:
            is_temp = True
            temp_name = into_match.group(1)
            raw_target = f"#{temp_name}"
            logger.debug(f"[DIAG] _parse_select_into: Detected temp table {raw_target} from original SQL")
    except Exception:
        pass
    
    # If not found in SQL, try to extract from AST
    if not raw_target:
        if hasattr(into_expr, 'this'):
            raw_target = self._get_table_name(into_expr.this, object_hint)
            # Check if it's a temp table by checking if name matches temp pattern
            # SQLGlot may have converted #table to TEMPORARY table
            if hasattr(into_expr, 'temporary') and getattr(into_expr, 'temporary', False):
                is_temp = True
                if not raw_target.startswith('#') and 'tempdb' not in raw_target.lower():
                    raw_target = f"#{raw_target.split('.')[-1]}"
        else:
            raw_target = object_hint or "unknown"
    
    # Final check: if raw_target doesn't start with # but we detected it as temp, add #
    if is_temp and raw_target and not raw_target.startswith('#') and 'tempdb' not in raw_target.lower():
        raw_target = f"#{raw_target.split('.')[-1]}"
    logger.debug(f"[DIAG] _parse_select_into: Final raw_target={raw_target}, is_temp={is_temp}")
    try:
        parts = (raw_target or "").split('.')
        if len(parts) >= 3 and self.registry:
            db, sch, tbl = parts[0], parts[1], ".".join(parts[2:])
            self.registry.learn_from_targets(f"{sch}.{tbl}", db)
    except Exception:
        pass
    # Use correct obj_type_hint based on whether it's a temp table
    obj_type = "temp_table" if (is_temp or (raw_target and (raw_target.startswith('#') or 'tempdb' in raw_target.lower()))) else "table"
    ns, nm = self._ns_and_name(raw_target, obj_type_hint=obj_type)
    namespace = ns
    table_name = nm

    # Process CTEs before extracting lineage (CTEs need to be registered for column lineage extraction)
    # Check if this SELECT is part of a WITH statement (parent is exp.With)
    # If so, process CTEs from the parent
    logger.debug(f"[DIAG] _parse_select_into: About to check for CTEs; statement type={type(statement).__name__}")
    parent = getattr(statement, 'parent', None)
    if isinstance(parent, exp.With):
        logger.debug(f"[DIAG] _parse_select_into: Parent is exp.With, processing CTEs")
        # If parent is exp.With, the actual SELECT is in parent.this
        if hasattr(parent, 'this') and isinstance(parent.this, exp.Select):
            self._process_ctes(parent.this)
    elif isinstance(statement, exp.Select):
        # Check if statement has CTEs directly using args.get('with')
        with_clause = statement.args.get('with')
        if with_clause:
            logger.debug(f"[DIAG] _parse_select_into: Found WITH clause on SELECT statement")
            self._process_ctes(statement)
        else:
            logger.debug(f"[DIAG] _parse_select_into: No WITH clause on SELECT statement")
    elif isinstance(statement, exp.With) and hasattr(statement, 'this'):
        logger.debug(f"[DIAG] _parse_select_into: Statement itself is exp.With, processing CTEs")
        # If statement itself is exp.With, process CTEs from the SELECT inside
        if isinstance(statement.this, exp.Select):
            self._process_ctes(statement.this)
    else:
        logger.debug(f"[DIAG] _parse_select_into: No CTE processing path matched")
    
    # Log cte_registry state after processing
    if hasattr(self, 'cte_registry'):
        logger.debug(f"[DIAG] _parse_select_into: cte_registry has {len(self.cte_registry)} CTEs: {list(self.cte_registry.keys())}")
    else:
        logger.debug(f"[DIAG] _parse_select_into: cte_registry not found")

    dependencies = self._extract_dependencies(statement)
    logger.debug(f"_parse_select_into: dependencies={dependencies}, raw_target={raw_target}")
    
    # Use the actual SELECT statement for lineage extraction (handle WITH clause)
    select_stmt = statement.this if isinstance(statement, exp.With) else statement
    logger.debug(f"_parse_select_into: select_stmt type={type(select_stmt).__name__}, has_with={bool(select_stmt.args.get('with') if isinstance(select_stmt, exp.Select) else False)}")
    
    logger.debug(f"_parse_select_into: About to extract column lineage for {table_name}, select_stmt type: {type(select_stmt).__name__}")
    try:
        lineage, output_columns = self._extract_column_lineage(select_stmt, table_name)
        logger.debug(f"_parse_select_into: Successfully extracted lineage for {table_name}")
    except Exception as e:
        logger.debug(f"_parse_select_into: ERROR extracting lineage for {table_name}: {e}")
        import traceback
        logger.debug(f"_parse_select_into: Traceback: {traceback.format_exc()}")
        lineage = []
        output_columns = []
    
    # Fix for empty columns: if parser fails to extract columns (e.g. complex CTEs), default to *
    if not output_columns:
        logger.debug(f"_parse_select_into: No columns extracted for {table_name}, defaulting to '*'")
        output_columns = [ColumnSchema(name="*", data_type="unknown", ordinal=0, nullable=True)]

    # If SELECT has star expansion but only placeholder columns, keep wildcard instead of unknown_*
    try:
        has_star = isinstance(select_stmt, exp.Select) and self._has_star_expansion(select_stmt)
        col_names = [str(c.name) for c in (output_columns or []) if c and c.name]
        if has_star and _is_placeholder_cols(col_names) and "*" not in col_names:
            star_refs: List[ColumnReference] = []
            for select_expr in select_stmt.expressions:
                is_star = isinstance(select_expr, exp.Star)
                is_col_star = isinstance(select_expr, exp.Column) and (str(select_expr.this) == "*" or str(select_expr).endswith(".*"))
                if not (is_star or is_col_star):
                    continue
                alias = str(select_expr.table) if hasattr(select_expr, 'table') and select_expr.table else None
                if alias:
                    table_name_star = self._resolve_table_from_alias(alias, select_stmt)
                    if table_name_star and table_name_star != "unknown":
                        ns, nm = self._ns_and_name(table_name_star)
                        star_refs.append(ColumnReference(namespace=ns, table_name=nm, column_name="*"))
                else:
                    for tbl in select_stmt.find_all(exp.Table):
                        table_name_star = self._get_table_name(tbl)
                        if table_name_star and table_name_star != "unknown":
                            ns, nm = self._ns_and_name(table_name_star)
                            star_refs.append(ColumnReference(namespace=ns, table_name=nm, column_name="*"))
            if star_refs:
                seen = set()
                dedup_refs = []
                for ref in star_refs:
                    key = (ref.namespace, ref.table_name, ref.column_name)
                    if key not in seen:
                        seen.add(key)
                        dedup_refs.append(ref)
                lineage = [ColumnLineage(output_column="*", input_fields=dedup_refs, transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *")]
            output_columns = [ColumnSchema(name="*", data_type="unknown", ordinal=0, nullable=True)]
    except Exception:
        pass

    # If temp target uses qualified star (table.*), keep wildcard instead of expanding columns
    try:
        raw_target_str = str(raw_target) if raw_target is not None else ""
        is_temp_target = bool(is_temp or (raw_target_str and (raw_target_str.startswith('#') or '#' in raw_target_str or 'tempdb' in raw_target_str.lower())))
        if is_temp_target and isinstance(select_stmt, exp.Select):
            from ..parser_modules import select_lineage as _sl
            qualified_star_exprs = []
            non_star_exprs = []
            for expr in (select_stmt.expressions or []):
                is_star = isinstance(expr, exp.Star)
                is_col_star = isinstance(expr, exp.Column) and (str(expr.this) == "*" or str(expr).endswith(".*"))
                if is_star or is_col_star:
                    has_table = False
                    if isinstance(expr, exp.Column) and getattr(expr, "table", None):
                        has_table = True
                    elif isinstance(expr, exp.Star) and expr.args.get("this"):
                        has_table = True
                    if has_table:
                        qualified_star_exprs.append(expr)
                    continue
                non_star_exprs.append(expr)

            if qualified_star_exprs:
                current_names = [str(c.name) for c in (output_columns or []) if c and c.name]
                has_concrete = any(n and n != "*" and not n.lower().startswith("unknown_") for n in current_names)
                if not has_concrete:
                    non_star_names = []
                    for expr in non_star_exprs:
                        name = _sl._extract_column_alias(self, expr)
                        if name:
                            non_star_names.append(str(name).strip().strip("[]"))

                    non_star_lookup = {n.lower() for n in non_star_names if n}
                    existing_by_name = {}
                    for col in (output_columns or []):
                        if col and col.name:
                            existing_by_name[str(col.name).lower()] = col

                    new_output_columns = []
                    ordinal = 0
                    new_output_columns.append(ColumnSchema(name="*", data_type="unknown", ordinal=ordinal, nullable=True))
                    ordinal += 1
                    for name in non_star_names:
                        key = str(name).lower()
                        col = existing_by_name.get(key)
                        if not col:
                            col = ColumnSchema(name=name, data_type="unknown", ordinal=ordinal, nullable=True)
                        col.ordinal = ordinal
                        new_output_columns.append(col)
                        ordinal += 1

                    star_refs: List[ColumnReference] = []
                    for expr in qualified_star_exprs:
                        if isinstance(expr, exp.Column) and getattr(expr, "table", None):
                            alias = str(expr.table)
                            table_name_star = self._resolve_table_from_alias(alias, select_stmt)
                            if table_name_star and table_name_star != "unknown":
                                ns, nm = self._ns_and_name(table_name_star)
                                star_refs.append(ColumnReference(namespace=ns, table_name=nm, column_name="*"))
                        elif isinstance(expr, exp.Star) and expr.args.get("this"):
                            alias = str(expr.args.get("this"))
                            table_name_star = self._resolve_table_from_alias(alias, select_stmt)
                            if table_name_star and table_name_star != "unknown":
                                ns, nm = self._ns_and_name(table_name_star)
                                star_refs.append(ColumnReference(namespace=ns, table_name=nm, column_name="*"))

                    new_lineage = []
                    if lineage:
                        for ln in lineage:
                            if str(ln.output_column).lower() in non_star_lookup:
                                new_lineage.append(ln)
                    if star_refs:
                        seen = set()
                        dedup_refs = []
                        for ref in star_refs:
                            key = (ref.namespace, ref.table_name, ref.column_name)
                            if key not in seen:
                                seen.add(key)
                                dedup_refs.append(ref)
                        new_lineage.insert(0, ColumnLineage(output_column="*", input_fields=dedup_refs, transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))

                    output_columns = new_output_columns
                    lineage = new_lineage
    except Exception:
        pass

    # If temp target uses bare star (*), keep wildcard and non-star expressions only
    try:
        raw_target_str = str(raw_target) if raw_target is not None else ""
        is_temp_target = bool(is_temp or (raw_target_str and (raw_target_str.startswith('#') or '#' in raw_target_str or 'tempdb' in raw_target_str.lower())))
        if is_temp_target and isinstance(select_stmt, exp.Select):
            from ..parser_modules import select_lineage as _sl
            has_bare_star = False
            non_star_exprs = []
            for expr in (select_stmt.expressions or []):
                is_star = isinstance(expr, exp.Star)
                is_col_star = isinstance(expr, exp.Column) and (str(expr.this) == "*" or str(expr).endswith(".*"))
                if is_star or is_col_star:
                    has_table = False
                    if isinstance(expr, exp.Column) and getattr(expr, "table", None):
                        has_table = True
                    elif isinstance(expr, exp.Star) and expr.args.get("this"):
                        has_table = True
                    if not has_table:
                        has_bare_star = True
                    continue
                non_star_exprs.append(expr)

            if has_bare_star:
                current_names = [str(c.name) for c in (output_columns or []) if c and c.name]
                has_concrete = any(n and n != "*" and not n.lower().startswith("unknown_") for n in current_names)
                if not has_concrete:
                    non_star_names = []
                    for expr in non_star_exprs:
                        name = _sl._extract_column_alias(self, expr)
                        if name:
                            non_star_names.append(str(name).strip().strip("[]"))

                    non_star_lookup = {n.lower() for n in non_star_names if n}
                    existing_by_name = {}
                    for col in (output_columns or []):
                        if col and col.name:
                            existing_by_name[str(col.name).lower()] = col

                    include_star = not non_star_names
                    new_output_columns = []
                    ordinal = 0
                    if include_star:
                        new_output_columns.append(ColumnSchema(name="*", data_type="unknown", ordinal=ordinal, nullable=True))
                        ordinal += 1
                    for name in non_star_names:
                        key = str(name).lower()
                        col = existing_by_name.get(key)
                        if not col:
                            col = ColumnSchema(name=name, data_type="unknown", ordinal=ordinal, nullable=True)
                        col.ordinal = ordinal
                        new_output_columns.append(col)
                        ordinal += 1

                    # Build wildcard lineage from source tables, excluding INTO target
                    star_refs: List[ColumnReference] = []
                    target_table_name = None
                    try:
                        if hasattr(select_stmt, "args") and "into" in select_stmt.args:
                            into_expr = select_stmt.args["into"]
                            if into_expr and hasattr(into_expr, "this") and isinstance(into_expr.this, exp.Table):
                                target_table_name = self._get_table_name(into_expr.this)
                    except Exception:
                        target_table_name = None

                    for tbl in select_stmt.find_all(exp.Table):
                        table_name_star = self._get_table_name(tbl)
                        if table_name_star and table_name_star != "unknown":
                            if target_table_name and table_name_star == target_table_name:
                                continue
                            ns, nm = self._ns_and_name(table_name_star)
                            star_refs.append(ColumnReference(namespace=ns, table_name=nm, column_name="*"))

                    new_lineage = []
                    if lineage:
                        for ln in lineage:
                            if str(ln.output_column).lower() in non_star_lookup:
                                new_lineage.append(ln)
                    if include_star and star_refs:
                        seen = set()
                        dedup_refs = []
                        for ref in star_refs:
                            key = (ref.namespace, ref.table_name, ref.column_name)
                            if key not in seen:
                                seen.add(key)
                                dedup_refs.append(ref)
                        new_lineage.insert(0, ColumnLineage(output_column="*", input_fields=dedup_refs, transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))

                    output_columns = new_output_columns
                    lineage = new_lineage
    except Exception:
        pass

    # If wildcard is present, drop placeholder unknown_* columns from schema/lineage
    try:
        col_names = [str(c.name) for c in (output_columns or []) if c and c.name]
        has_wildcard = any(n == "*" for n in col_names)
        if has_wildcard:
            output_columns = [c for c in output_columns if not str(c.name).lower().startswith("unknown_")]
            if lineage:
                lineage = [ln for ln in lineage if not str(ln.output_column).lower().startswith("unknown_")]
    except Exception:
        pass

    # Ensure aliased expressions are represented in output schema/lineage
    try:
        if isinstance(select_stmt, exp.Select):
            from ..parser_modules import select_lineage as _sl
            alias_exprs = []
            for expr in (select_stmt.expressions or []):
                if isinstance(expr, exp.Alias):
                    alias_name = expr.alias or expr.alias_or_name
                    if alias_name:
                        alias_exprs.append((str(alias_name).strip().strip("[]"), expr))

            existing_lower = {str(c.name).lower() for c in (output_columns or []) if c and c.name}
            lineage_by_name = {str(ln.output_column).lower(): ln for ln in (lineage or []) if ln and ln.output_column}
            for alias_name, alias_expr in alias_exprs:
                if not alias_name:
                    continue
                alias_lower = alias_name.lower()
                if alias_lower not in existing_lower:
                    output_columns.append(ColumnSchema(name=alias_name, data_type="unknown", ordinal=len(output_columns), nullable=True))
                    existing_lower.add(alias_lower)
                if alias_lower not in lineage_by_name:
                    inner = alias_expr.this
                    input_refs = _sl._extract_column_references(self, inner, select_stmt)
                    if isinstance(inner, exp.Case):
                        ttype = TransformationType.CASE
                    else:
                        ttype = TransformationType.EXPRESSION
                    lineage.append(ColumnLineage(output_column=alias_name, input_fields=input_refs, transformation_type=ttype, transformation_description=f"SELECT {str(alias_expr)}"))
                    lineage_by_name[alias_lower] = lineage[-1]

            for idx, col in enumerate(output_columns):
                col.ordinal = idx
    except Exception:
        pass

    # Remove spurious columns that match table aliases/names (e.g., CTE alias split into columns)
    try:
        if isinstance(select_stmt, exp.Select) and output_columns:
            alias_names = set()
            for tbl in select_stmt.find_all(exp.Table):
                try:
                    if getattr(tbl, "alias", None):
                        alias_names.add(str(tbl.alias))
                except Exception:
                    pass
                try:
                    if getattr(tbl, "name", None):
                        alias_names.add(str(tbl.name))
                except Exception:
                    pass
            alias_names = {a.strip("[]").lower() for a in alias_names if a}

            if not alias_names:
                try:
                    import re as _re
                    sql_text = str(select_stmt)
                    for m in _re.finditer(r"\bFROM\s+([#\w\.\[\]]+)(?:\s+(?:AS\s+)?([#\w]+))?", sql_text, _re.IGNORECASE):
                        alias = m.group(2) or m.group(1)
                        if alias:
                            alias_names.add(str(alias).strip("[]").lower())
                    for m in _re.finditer(r"\bJOIN\s+([#\w\.\[\]]+)(?:\s+(?:AS\s+)?([#\w]+))?", sql_text, _re.IGNORECASE):
                        alias = m.group(2) or m.group(1)
                        if alias:
                            alias_names.add(str(alias).strip("[]").lower())
                except Exception:
                    pass

            if alias_names:
                keep_cols = []
                keep_names = set()
                for col in output_columns:
                    if not col or not col.name:
                        continue
                    name = str(col.name).strip("[]")
                    if name.lower() in alias_names:
                        continue
                    keep_cols.append(col)
                    keep_names.add(name.lower())

                if keep_cols and len(keep_cols) < len(output_columns):
                    output_columns = keep_cols
                    if lineage:
                        lineage = [ln for ln in lineage if ln and ln.output_column and str(ln.output_column).strip("[]").lower() in keep_names]
                    for idx, col in enumerate(output_columns):
                        col.ordinal = idx
    except Exception:
        pass
        
    logger.debug(f"_parse_select_into: lineage count={len(lineage or [])}, output_columns count={len(output_columns or [])}")
    if lineage:
        for lin in lineage[:3]:  # Show first 3 lineage items
            logger.debug(f"_parse_select_into: lineage item: {lin.output_column} -> {len(lin.input_fields or [])} input_fields")
    else:
        logger.debug(f"_parse_select_into: No lineage extracted for {table_name}")

    # Build final_dependencies first (with CTE expansion) before using it for temp_sources
    final_dependencies: Set[str] = set()
    for d in dependencies:
        is_dep_temp = ('#' in d or 'tempdb' in d.lower())
        # Check if this dependency is a CTE (should not be added as a dependency)
        # Only check if cte_registry exists and is not empty
        is_cte = False
        if hasattr(self, 'cte_registry') and self.cte_registry:
            dep_simple = d.split('.')[-1] if '.' in d else d
            # Only treat as CTE if it's explicitly in cte_registry (case-insensitive)
            cte_registry_lower = {k.lower(): k for k in self.cte_registry.keys()}
            is_cte = dep_simple and dep_simple.lower() in cte_registry_lower
        if is_cte:
            # This is a CTE - don't add it as a dependency, expand it to base sources instead
            dep_simple = d.split('.')[-1] if '.' in d else d
            logger.debug(f"_parse_select_into: Skipping CTE {dep_simple} from dependencies, will expand to base sources")
            cte_name = cte_registry_lower.get(dep_simple.lower())
            if cte_name:
                cte_info = self.cte_registry.get(cte_name)
                if cte_info:
                    if isinstance(cte_info, dict) and 'definition' in cte_info:
                        cte_def = cte_info['definition']
                    elif isinstance(cte_info, exp.Select):
                        cte_def = cte_info
                    else:
                        cte_def = None
                    if cte_def and isinstance(cte_def, exp.Select):
                        cte_deps = self._extract_dependencies(cte_def)
                        # Add base sources from CTE (expand temp tables to their base sources, exclude CTEs)
                        for cte_dep in cte_deps:
                            cte_dep_simple = cte_dep.split('.')[-1] if '.' in cte_dep else cte_dep
                            is_cte_dep_temp = cte_dep_simple.startswith('#') or (f"#{cte_dep_simple}" in self.temp_registry)
                            is_cte_dep_cte = cte_dep_simple and cte_dep_simple.lower() in cte_registry_lower
                            if is_cte_dep_temp:
                                # Expand temp table to its base sources
                                temp_key = cte_dep_simple if cte_dep_simple.startswith('#') else f"#{cte_dep_simple}"
                                temp_bases = self.temp_sources.get(temp_key, set())
                                if temp_bases:
                                    final_dependencies.update(temp_bases)
                                    logger.debug(f"_parse_select_into: Expanded temp table {cte_dep} to base sources: {temp_bases}")
                                else:
                                    # If no base sources found, add temp table itself
                                    final_dependencies.add(cte_dep)
                            elif not is_cte_dep_cte:
                                final_dependencies.add(cte_dep)
                                logger.debug(f"_parse_select_into: Added base source {cte_dep} from CTE {cte_name}")
            continue
        if not is_dep_temp:
            final_dependencies.add(d)
        else:
            final_dependencies.add(d)
            dep_simple = self._extract_temp_name(d) if '#' in d else d.split('.')[-1]
            if not dep_simple.startswith('#'):
                dep_simple = f"#{dep_simple}"
            dep_bases = self.temp_sources.get(dep_simple, set())
            if dep_bases:
                final_dependencies.update(dep_bases)

    if raw_target and (raw_target.startswith('#') or 'tempdb..#' in str(raw_target)):
        simple_key = self._extract_temp_name(raw_target if '#' in raw_target else '#' + raw_target)
        if not simple_key.startswith('#'):
            simple_key = f"#{simple_key}"
        namespace, table_name = self._ns_and_name(simple_key, obj_type_hint="temp_table")
        temp_cols = [col.name for col in output_columns]
        existing_cols = self.temp_registry.get(simple_key)
        ver_key = self._temp_next(simple_key)
        ver_cols = temp_cols
        if _is_placeholder_cols(temp_cols) and existing_cols and not _is_placeholder_cols(existing_cols):
            ver_cols = existing_cols
        self.temp_registry[ver_key] = ver_cols
        if _should_update_temp_cols(existing_cols, temp_cols):
            self.temp_registry[simple_key] = temp_cols
        else:
            logger.debug(f"_parse_select_into: Skipping temp_registry overwrite for {simple_key}, existing_cols={len(existing_cols or [])}, new_cols={len(temp_cols)}")
        base_sources: Set[str] = set()
        # Use final_dependencies (with CTE expansion) instead of dependencies
        for d in final_dependencies:
            is_dep_temp = ('#' in d or 'tempdb' in d.lower())
            if not is_dep_temp:
                base_sources.add(d)
            else:
                dep_simple = self._extract_temp_name(d) if '#' in d else d.split('.')[-1]
                if not dep_simple.startswith('#'):
                    dep_simple = f"#{dep_simple}"
                dep_bases = self.temp_sources.get(dep_simple, set())
                if dep_bases:
                    base_sources.update(dep_bases)
                else:
                    base_sources.add(d)
        self.temp_sources[simple_key] = base_sources
        try:
            col_map = {lin.output_column: list(lin.input_fields or []) for lin in (lineage or [])}
            logger.debug(f"_parse_select_into: temp_lineage for {simple_key}: {len(col_map)} columns")
            for col_name, refs in list(col_map.items())[:3]:  # Show first 3 columns
                logger.debug(f"_parse_select_into: temp_lineage[{simple_key}][{col_name}]: {len(refs)} references")
            self.temp_lineage[ver_key] = col_map
            self.temp_lineage[simple_key] = col_map
            logger.debug(f"_parse_select_into: Stored temp_lineage for {simple_key}: {len(col_map)} columns, ver_key={ver_key}")
        except Exception as e:
            logger.debug(f"_parse_select_into: Exception storing temp_lineage: {e}")
            logger.debug(f"_parse_select_into: Exception storing temp_lineage for {simple_key}: {e}")
            pass

    schema = TableSchema(namespace=namespace, name=table_name, columns=output_columns)
    self.schema_registry.register(schema)

    return ObjectInfo(
        name=table_name,
        object_type="temp_table" if (raw_target and (raw_target.startswith('#') or 'tempdb..#' in raw_target)) else "table",
        schema=schema,
        lineage=lineage,
        dependencies=final_dependencies
    )


def _parse_insert_exec(self, statement: exp.Insert, object_hint: Optional[str] = None) -> ObjectInfo:
    raw_target = self._get_table_name(statement.this, object_hint)
    try:
        parts = (raw_target or "").split('.')
        if len(parts) >= 3 and self.registry:
            db, sch, tbl = parts[0], parts[1], ".".join(parts[2:])
            self.registry.learn_from_targets(f"{sch}.{tbl}", db)
    except Exception:
        pass
    ns, nm = self._ns_and_name(raw_target, obj_type_hint="table")
    namespace = ns
    table_name = nm

    expression = statement.expression
    if hasattr(expression, 'expressions') and expression.expressions:
        exec_command = expression.expressions[0]
        dependencies = set()
        procedure_name = None
        exec_text = str(exec_command)
        if exec_text.upper().startswith('EXEC'):
            parts = exec_text.split()
            if len(parts) > 1:
                raw_proc_name = self._clean_proc_name(parts[1])
                procedure_name = self._get_full_table_name(raw_proc_name)
                dependencies.add(procedure_name)
        target_columns: List[ColumnSchema] = []
        try:
            cols_arg = statement.args.get('columns') if hasattr(statement, 'args') else None
            if cols_arg:
                for i, c in enumerate(cols_arg or []):
                    name = None
                    if hasattr(c, 'name') and getattr(c, 'name'):
                        name = str(getattr(c, 'name'))
                    elif hasattr(c, 'this'):
                        name = str(getattr(c, 'this'))
                    else:
                        name = str(c)
                        if name:
                            target_columns.append(ColumnSchema(name=str(name).strip('[]'), data_type="unknown", ordinal=i, nullable=True))
        except Exception:
            target_columns = []
        output_columns = target_columns or [
            ColumnSchema(name="output_col_1", data_type="unknown", ordinal=0, nullable=True),
            ColumnSchema(name="output_col_2", data_type="unknown", ordinal=1, nullable=True),
        ]
        if raw_target and (str(raw_target).startswith('#') or 'tempdb..#' in str(raw_target)):
                # Canonical simple temp key (e.g., '#temp')
                simple_key = (str(raw_target).split('.')[-1] if '.' in str(raw_target) else str(raw_target))
                if not simple_key.startswith('#'):
                    simple_key = f"#{simple_key}"
                # Output naming for temp materialization: DB.schema.<object_hint>.#temp
                db = self.current_database or self.default_database or "InfoTrackerDW"
                sch = getattr(self, 'default_schema', None) or "dbo"
                label = (object_hint or "object")
                table_name = f"{db}.{sch}.{label}.{simple_key}"
                namespace = self._canonical_namespace(db)
        lineage = []
        if procedure_name:
            ns_proc, nm_proc = self._ns_and_name(procedure_name)
            for i, col in enumerate(output_columns):
                input_col = col.name if target_columns else "*"
                lineage.append(ColumnLineage(
                    output_column=col.name,
                    input_fields=[ColumnReference(namespace=ns_proc, table_name=nm_proc, column_name=input_col)],
                    transformation_type=TransformationType.EXEC,
                    transformation_description=f"INSERT INTO {table_name} EXEC {nm_proc}"
                ))
        schema = TableSchema(namespace=namespace, name=table_name, columns=output_columns)
        self.schema_registry.register(schema)
        return ObjectInfo(
            name=table_name,
            object_type="temp_table" if (raw_target and (str(raw_target).startswith('#') or 'tempdb..#' in str(raw_target))) else "table",
            schema=schema,
            lineage=lineage,
            dependencies=dependencies
        )
    raise ValueError("Could not parse INSERT INTO ... EXEC statement")


def _parse_insert_select(self, statement: exp.Insert, object_hint: Optional[str] = None) -> Optional[ObjectInfo]:
    import logging
    logger = logging.getLogger(__name__)
    from ..openlineage_utils import sanitize_name
    raw_target = self._get_table_name(statement.this, object_hint)
    try:
        parts = (raw_target or "").split('.')
        if len(parts) >= 3 and self.registry:
            db, sch, tbl = parts[0], parts[1], ".".join(parts[2:])
            self.registry.learn_from_targets(f"{sch}.{tbl}", db)
    except Exception:
        pass
    ns, nm = self._ns_and_name(raw_target, obj_type_hint="table")
    namespace = ns
    table_name = nm
    select_expr = statement.expression
    if not isinstance(select_expr, exp.Select):
        logger.debug(f"_parse_insert_select: select_expr is not a Select, type={type(select_expr).__name__}")
        # Try to get SELECT from statement.args if expression is None
        if select_expr is None:
            select_expr = statement.args.get('expression')
            if isinstance(select_expr, exp.Select):
                logger.debug(f"_parse_insert_select: Found SELECT in statement.args.expression")
            else:
                # Try to get SELECT from statement.args.get('this') or statement.args.get('query')
                select_expr = statement.args.get('this') or statement.args.get('query')
                if isinstance(select_expr, exp.Select):
                    logger.debug(f"_parse_insert_select: Found SELECT in statement.args.this/query")
        if not isinstance(select_expr, exp.Select):
            # Fallback: try to extract lineage from SQL string
            logger.debug(f"_parse_insert_select: Still no Select found, trying string fallback")
            try:
                insert_sql = str(statement)
                logger.debug(f"_parse_insert_select: INSERT SQL (first 200 chars): {insert_sql[:200]}")
                # Use string-based fallback
                # Use raw_target for matching (actual table name from SQL) instead of table_name (which may have procedure prefix)
                logger.debug(f"_parse_insert_select: Calling _extract_insert_select_lineage_string with table_name={table_name}, raw_target={raw_target}")
                # Try with raw_target first (actual table name from SQL), then fallback to table_name
                lineage, deps = self._extract_insert_select_lineage_string(insert_sql, raw_target or table_name)
                logger.debug(f"_parse_insert_select: _extract_insert_select_lineage_string returned {len(lineage)} columns, {len(deps)} dependencies")
                if lineage or deps:
                    logger.debug(f"_parse_insert_select: String fallback found {len(lineage)} columns, {len(deps)} dependencies")
                    # Get output columns from INSERT column list or infer from lineage
                    output_columns = []
                    if hasattr(statement, 'this') and hasattr(statement.this, 'expressions'):
                        # Try to get columns from INSERT column list
                        for col_expr in statement.this.expressions:
                            if hasattr(col_expr, 'name'):
                                output_columns.append(ColumnSchema(name=col_expr.name, data_type='unknown', nullable=True, ordinal=len(output_columns)))
                    if not output_columns:
                        # Infer from lineage
                        output_columns = [ColumnSchema(name=lin.output_column, data_type='unknown', nullable=True, ordinal=i) for i, lin in enumerate(lineage)]
                    if not output_columns:
                        # Last resort: try to extract from SQL
                        from ..parser_modules import string_fallbacks as _sf
                        output_columns = _sf._extract_basic_select_columns(insert_sql)
                    schema = TableSchema(namespace=namespace, name=table_name, columns=output_columns)
                    self.schema_registry.register(schema)
                    return ObjectInfo(
                        name=table_name,
                        object_type="table",
                        schema=schema,
                        lineage=lineage,
                        dependencies=deps
                    )
            except Exception as fallback_error:
                logger.debug(f"_parse_insert_select: String fallback failed: {fallback_error}")
                import traceback
                logger.debug(f"_parse_insert_select: Traceback: {traceback.format_exc()}")
            logger.debug(f"_parse_insert_select: Still no Select found, returning None")
            return None
    
    # Check if INSERT statement has a parent WITH clause (CTE defined before INSERT)
    # SQLGlot may parse "WITH ... INSERT INTO ..." as exp.With containing exp.Insert
    parent = getattr(statement, 'parent', None)
    if isinstance(parent, exp.With):
        # Process CTEs from the parent WITH clause
        self._process_ctes(parent.this if hasattr(parent, 'this') and isinstance(parent.this, exp.Select) else parent)
        logger.debug(f"_parse_insert_select: Processed CTEs from parent WITH, cte_registry keys: {list(self.cte_registry.keys())}")
    # Also check if the INSERT statement itself has a WITH clause
    elif hasattr(statement, 'with') and getattr(statement, 'with', None):
        self._process_ctes(statement)
        logger.debug(f"_parse_insert_select: Processed CTEs from INSERT WITH, cte_registry keys: {list(self.cte_registry.keys())}")
    
    dependencies = self._extract_dependencies(select_expr)
    logger.debug(f"_parse_insert_select: target={table_name}, dependencies={dependencies}")
    
    # Process CTEs before extracting lineage (CTEs need to be registered for column lineage extraction)
    if isinstance(select_expr, exp.Select):
        # Check if statement has CTEs directly using args.get('with')
        with_clause = select_expr.args.get('with')
        if with_clause:
            self._process_ctes(select_expr)
            logger.debug(f"_parse_insert_select: Processed CTEs from SELECT WITH, cte_registry keys: {list(self.cte_registry.keys())}")
    elif isinstance(select_expr, exp.With) and hasattr(select_expr, 'this'):
        # If statement itself is exp.With, process CTEs from the SELECT inside
        if isinstance(select_expr.this, exp.Select):
            self._process_ctes(select_expr.this)
            logger.debug(f"_parse_insert_select: Processed CTEs from SELECT WITH, cte_registry keys: {list(self.cte_registry.keys())}")
    
    # For INSERT INTO, we need to use direct sources from FROM clause, not expanded temp_lineage
    # Set flag to use direct references for temp tables
    old_use_direct_ref = getattr(self, '_use_direct_temp_ref', False)
    self._use_direct_temp_ref = True
    
    lineage, output_columns = self._extract_column_lineage(select_expr, table_name)
    logger.debug(f"_parse_insert_select: lineage has {len(lineage)} columns, first few: {[(l.output_column, [str(f) for f in (l.input_fields or [])[:3]]) for l in lineage[:5]]}")
    
    # Restore flag
    self._use_direct_temp_ref = old_use_direct_ref
    
    table_name = sanitize_name(table_name)
    raw_is_temp = bool(raw_target and (str(raw_target).startswith('#') or 'tempdb' in str(raw_target)))
    if raw_is_temp:
        simple_name = (str(raw_target).split('.')[-1] if '.' in str(raw_target) else str(raw_target))
        if not simple_name.startswith('#'):
            simple_name = f"#{simple_name}"
        namespace, table_name = self._ns_and_name(simple_name, obj_type_hint="temp_table")
        temp_cols = [col.name for col in output_columns]
        existing_cols = self.temp_registry.get(simple_name)
        if _should_update_temp_cols(existing_cols, temp_cols):
            self.temp_registry[simple_name] = temp_cols
        else:
            logger.debug(f"_parse_insert_select: Skipping temp_registry overwrite for {simple_name}, existing_cols={len(existing_cols or [])}, new_cols={len(temp_cols)}")
        base_sources: Set[str] = set()
        for d in dependencies:
            is_dep_temp = ('#' in d or 'tempdb' in d.lower())
            if not is_dep_temp:
                base_sources.add(d)
            else:
                dep_simple = (d.split('.')[-1] if '.' in d else d)
                if not dep_simple.startswith('#'):
                    dep_simple = f"#{dep_simple}"
                dep_bases = self.temp_sources.get(dep_simple, set())
                if dep_bases:
                    base_sources.update(dep_bases)
                else:
                    base_sources.add(d)
        self.temp_sources[simple_name] = base_sources
        try:
            col_map = {lin.output_column: list(lin.input_fields or []) for lin in (lineage or [])}
            self.temp_lineage[simple_name] = col_map
        except Exception:
            pass

    schema = TableSchema(namespace=namespace, name=table_name, columns=output_columns)
    self.schema_registry.register(schema)

    final_dependencies: Set[str] = set()
    for d in dependencies:
        is_dep_temp = ('#' in d or 'tempdb' in d.lower())
        if not is_dep_temp:
            final_dependencies.add(d)
        else:
            final_dependencies.add(d)
            dep_simple = (d.split('.')[-1] if '.' in d else d)
            if not dep_simple.startswith('#'):
                dep_simple = f"#{dep_simple}"
            dep_bases = self.temp_sources.get(dep_simple, set())
            if dep_bases:
                final_dependencies.update(dep_bases)

    return ObjectInfo(
        name=table_name,
        object_type="temp_table" if raw_is_temp else "table",
        schema=schema,
        lineage=lineage,
        dependencies=final_dependencies
    )

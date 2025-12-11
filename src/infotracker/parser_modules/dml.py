from __future__ import annotations

from typing import Optional, List, Set
from sqlglot import expressions as exp

from ..models import ObjectInfo, TableSchema, ColumnSchema, ColumnLineage, ColumnReference, TransformationType


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
    lineage, output_columns = self._extract_column_lineage(select_stmt, table_name)
    
    # Fix for empty columns: if parser fails to extract columns (e.g. complex CTEs), default to *
    if not output_columns:
        logger.debug(f"_parse_select_into: No columns extracted for {table_name}, defaulting to '*'")
        output_columns = [ColumnSchema(name="*", data_type="unknown", ordinal=0, nullable=True)]
        
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
        ver_key = self._temp_next(simple_key)
        self.temp_registry[ver_key] = temp_cols
        self.temp_registry[simple_key] = temp_cols
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
        self.temp_registry[simple_name] = temp_cols
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

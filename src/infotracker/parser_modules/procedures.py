from __future__ import annotations
from typing import Optional, List, Set
import re
import logging

import sqlglot
from sqlglot import exp  # type: ignore

from ..models import ObjectInfo, TableSchema, ColumnSchema, ColumnLineage

logger = logging.getLogger(__name__)


def _parse_procedure_string(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
    """Parse CREATE PROCEDURE using string-based approach (extracted)."""
    logger.debug(f"_parse_procedure_string: Called with object_hint={object_hint}")
    # Normalize headers (SET/GO, COLLATE, etc.)
    sql_content = self._normalize_tsql(sql_content)

    # Determine DB context from USE at the start
    # NOTE: current_database should already be set by parse_sql_file from USE statement
    # But we check again here in case this function is called directly
    try:
        db_from_use = self._extract_database_from_use_statement(sql_content)
        if db_from_use:
            self.current_database = db_from_use
    except Exception:
        pass

    procedure_name = self._extract_procedure_name(sql_content) or object_hint or "unknown_procedure"

    # Infer DB (prefer USE, else content) and set up canonical namespace
    inferred_db = self._infer_database_for_object(statement=None, sql_text=sql_content) or self.current_database or self.default_database
    namespace = self._canonical_namespace(inferred_db)

    # --- Establish parsing context for canonical temp naming (parity with legacy dev_parser) ---
    # IMPORTANT: Set context BEFORE any parsing that might create temp tables
    prev_ctx_db, prev_ctx_obj = getattr(self, "_ctx_db", None), getattr(self, "_ctx_obj", None)
    self._ctx_db = inferred_db or self.current_database or self.default_database
    self._ctx_obj = self._normalize_table_name_for_output(procedure_name)
    logger.debug(f"_parse_procedure_string: Set context at start: _ctx_db={self._ctx_db}, _ctx_obj={self._ctx_obj}, procedure_name={procedure_name}")

    # --- Prescan AST for temp materializations to register temp lineage early ---
    try:
        # Use the same preprocessing pipeline as the main parser so sqlglot can handle T-SQL procs
        normalized = self._normalize_tsql(sql_content)
        preprocessed = self._preprocess_sql(normalized)
        stmts = sqlglot.parse(preprocessed, read=self.dialect) or []
        logger.debug(f"_parse_procedure_body_statements: Parsed {len(stmts)} statements")
        for i, st in enumerate(stmts):
            st_type = type(st).__name__
            logger.debug(f"_parse_procedure_body_statements: Statement {i+1}: {st_type}")
            # Top-level SELECT ... INTO #tmp
            if isinstance(st, exp.Select) and self._is_select_into(st):
                logger.debug(f"_parse_procedure_body_statements: Found top-level SELECT INTO")
                self._parse_select_into(st, object_hint)
            # Top-level INSERT INTO #tmp SELECT ...
            if isinstance(st, exp.Insert):
                try:
                    if hasattr(st, 'expression') and isinstance(st.expression, exp.Select):
                        self._parse_insert_select(st, object_hint)
                except Exception:
                    pass
            # Handle WITH ... INSERT INTO ... statements
            elif isinstance(st, exp.With):
                # Check if the WITH contains an INSERT statement
                if hasattr(st, 'this'):
                    if isinstance(st.this, exp.Insert):
                        # Process CTEs from the WITH clause before parsing INSERT
                        self._process_ctes(st)
                        logger.debug(f"_parse_procedure_body_statements: Processed CTEs from WITH ... INSERT, cte_registry keys: {list(self.cte_registry.keys())}")
                        # Now parse the INSERT
                        if hasattr(st.this, 'expression') and isinstance(st.this.expression, exp.Select):
                            self._parse_insert_select(st.this, object_hint)
            if isinstance(st, exp.Create):
                logger.debug(f"_parse_procedure_body_statements: Found CREATE statement, walking nodes...")
                node_count = 0
                with_count = 0
                select_count = 0
                select_into_count = 0
                for node in st.walk():
                    node_count += 1
                    node_type = type(node).__name__
                    # Log first few nodes to see what we're getting
                    if node_count <= 10:
                        logger.debug(f"_parse_procedure_body_statements: Node {node_count}: {node_type}")
                    
                    # Handle WITH statement that contains SELECT INTO
                    if isinstance(node, exp.With):
                        with_count += 1
                        logger.debug(f"_parse_procedure_body_statements: Found WITH node #{with_count}")
                        # Check if the SELECT inside WITH has INTO
                        if hasattr(node, 'this') and isinstance(node.this, exp.Select):
                            logger.debug(f"_parse_procedure_body_statements: WITH node has SELECT as this")
                            if self._is_select_into(node.this):
                                select_into_count += 1
                                logger.debug(f"_parse_procedure_body_statements: Found SELECT INTO #{select_into_count} inside WITH")
                                self._parse_select_into(node.this, object_hint)
                            else:
                                logger.debug(f"_parse_procedure_body_statements: SELECT inside WITH does not have INTO")
                    # Nested SELECT ... INTO #tmp inside CREATE PROCEDURE body
                    elif isinstance(node, exp.Select):
                        select_count += 1
                        if self._is_select_into(node):
                            select_into_count += 1
                            logger.debug(f"_parse_procedure_body_statements: Found SELECT INTO node #{select_into_count}: {node_type}")
                        self._parse_select_into(node, object_hint)
                    # Nested INSERT INTO #tmp SELECT ... inside CREATE PROCEDURE body
                    elif isinstance(node, exp.Insert):
                        try:
                            if hasattr(node, 'expression') and isinstance(node.expression, exp.Select):
                                self._parse_insert_select(node, object_hint)
                        except Exception:
                            pass
                logger.debug(f"_parse_procedure_body_statements: Total nodes: {node_count}, WITH: {with_count}, SELECT: {select_count}, SELECT INTO: {select_into_count}")
    except Exception:
        pass

    # 1) Check if procedure materializes (SELECT INTO / INSERT INTO ... SELECT / TRUNCATE)
    materialized_outputs = self._extract_materialized_output_from_procedure_string(sql_content)
    if materialized_outputs:
        # Log how many outputs were found
        logger.debug(f"Found {len(materialized_outputs)} materialized output(s) for procedure {procedure_name}")
        # Process all outputs but return first one for backward compatibility
        # In future, engine should handle multiple outputs per procedure
        for idx, materialized_output in enumerate(materialized_outputs):
            logger.debug(f"  Output {idx+1}/{len(materialized_outputs)}: {materialized_output.name} ({materialized_output.object_type})")
        
        # Select the best output candidate
        # 1. Prefer outputs with lineage
        # 2. Prefer outputs that match the procedure name (heuristic)
        # 3. Fallback to first output
        
        best_output = materialized_outputs[0]
        
        if len(materialized_outputs) > 1:
            # Filter candidates with lineage
            candidates_with_lineage = [o for o in materialized_outputs if o.lineage]
            
            if candidates_with_lineage:
                # If we have candidates with lineage, pick the one that matches procedure name best
                # or just the first one with lineage if no name match
                best_output = candidates_with_lineage[0]
                
                # Try to match name
                proc_simple = procedure_name.split('.')[-1].lower().strip('[]')
                for cand in candidates_with_lineage:
                    cand_simple = cand.name.split('.')[-1].lower().strip('[]')
                    # Check if candidate name is contained in procedure name or vice versa
                    # e.g. update_asefl_TrialBalance_BV vs TrialBalance_asefl_BV
                    # Remove 'update_' prefix from proc name for better matching
                    proc_core = proc_simple.replace('update_', '').replace('sp_', '')
                    if cand_simple in proc_core or proc_core in cand_simple:
                        best_output = cand
                        break
            else:
                # No lineage, try to match name among all candidates
                proc_simple = procedure_name.split('.')[-1].lower().strip('[]')
                for cand in materialized_outputs:
                    cand_simple = cand.name.split('.')[-1].lower().strip('[]')
                    proc_core = proc_simple.replace('update_', '').replace('sp_', '')
                    if cand_simple in proc_core or proc_core in cand_simple:
                        best_output = cand
                        break
        
        materialized_output = best_output
        logger.debug(f"Selected primary output: {materialized_output.name} (lineage count: {len(materialized_output.lineage)})")
        # Ensure materialized output uses the correct database from USE statement
        if materialized_output.schema:
            # Update namespace to use current_database (from USE statement)
            correct_db = self.current_database or self.default_database or "InfoTrackerDW"
            materialized_output.schema.namespace = self._canonical_namespace(correct_db)
            # Update name to include correct database if needed
            if materialized_output.schema.name:
                parts = materialized_output.schema.name.split('.')
                if len(parts) == 2:  # schema.table
                    # Name is already correct (schema.table), namespace has the DB
                    pass
                elif len(parts) == 1:  # table only
                    materialized_output.schema.name = f"dbo.{parts[0]}"
            materialized_output.name = materialized_output.schema.name
        # Specialized parser: INSERT INTO ... SELECT -> compute lineage from that SELECT
        # Use the table name from materialized_output, not procedure_name, to match the INSERT INTO target
        target_table_name = materialized_output.schema.name if materialized_output.schema else materialized_output.name
        # If lineage was already extracted in _extract_materialized_output_from_procedure_string and has input_fields, use it
        # Otherwise, try to extract lineage again
        has_valid_lineage = materialized_output.lineage and any(lin.input_fields for lin in materialized_output.lineage)
        if not has_valid_lineage:
            try:
                ins_lineage, ins_deps = self._extract_insert_select_lineage_string(sql_content, target_table_name)
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
            # CREATE TABLE should only affect column order (ordinal), not lineage
            # Update column ordinals from CREATE TABLE, but preserve lineage
            # Map lineage columns to CREATE TABLE columns by name to update ordinals
            lineage_by_col = {col.output_column.lower(): col for col in (materialized_output.lineage or [])}
            
            # Update column ordinals from CREATE TABLE
            updated_columns = []
            for i, known_col in enumerate(known.columns):
                # Update ordinal in lineage if column exists
                lineage_col = lineage_by_col.get(known_col.name.lower())
                if lineage_col:
                    # Ensure output_column name matches (case may differ)
                    lineage_col.output_column = known_col.name
                
                # Create column schema with ordinal from CREATE TABLE
                updated_columns.append(ColumnSchema(
                    name=known_col.name,
                    data_type=known_col.data_type,
                    nullable=known_col.nullable,
                    ordinal=i  # Use ordinal from CREATE TABLE
                ))
            
            # Update schema columns but preserve lineage and other schema properties
            materialized_output.schema.columns = updated_columns
            # Keep namespace and name from materialized_output (may have been resolved differently)
            materialized_output.schema.namespace = ns
            materialized_output.schema.name = name_key
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

        # Supplement temp lineage/deps using lightweight segment parsing when AST walk wasn't possible
        try:
            src_text = sql_content
            seg_sql = self._preprocess_sql(self._normalize_tsql(src_text))
            import re as _re
            # SELECT ... INTO #temp ... segments
            # Use backwards-search approach: find INTO, then find its SELECT
            # This avoids matching wrong SELECT to INTO in multi-statement procedures
            select_into_matches = list(_re.finditer(r'INTO\s+#([A-Za-z0-9_]+)', src_text, _re.IGNORECASE))
            
            for m_into in select_into_matches:
                into_pos = m_into.start()
                into_end = m_into.end()
                tmp = m_into.group(1)
                
                # Find the SELECT that belongs to this INTO
                # Strategy: search backwards through statement boundaries (double-newlines)
                # until we find a SELECT keyword
                sql_before_into = src_text[:into_pos]
                select_pos = None
                
                # Search backwards through all double-newline boundaries
                search_pos = len(sql_before_into)
                while search_pos > 0:
                    # Find the last double-newline before current search position
                    boundary_pos = sql_before_into.rfind('\n\n', 0, search_pos)
                    search_from = 0 if boundary_pos == -1 else boundary_pos
                    
                    # Search for SELECT in this range
                    select_search_text = sql_before_into[search_from:search_pos]
                    select_matches_list = list(_re.finditer(r'\bSELECT\b', select_search_text, _re.IGNORECASE))
                    
                    if select_matches_list:
                        # Found a SELECT - use the last one in this segment
                        last_select = select_matches_list[-1]
                        select_pos = search_from + last_select.start()
                        break
                    
                    # Move search position to this boundary and continue looking further back
                    search_pos = boundary_pos
                    if boundary_pos == -1:
                        # No more boundaries - no SELECT found
                        break
                
                if select_pos is None:
                    continue
                
                # Find end of INTO statement
                after_into = src_text[into_end:]
                end_match = _re.search(r'\n\s*(?:GO|;|SELECT\s+@)', after_into, _re.IGNORECASE | _re.MULTILINE)
                if end_match:
                    end_pos = into_end + end_match.start()
                else:
                    double_nl = after_into.find('\n\n')
                    end_pos = (into_end + double_nl) if double_nl > 0 else len(src_text)
                
                raw_seg = src_text[select_pos:end_pos]
                
                # Try AST on the normalized/preprocessed segment

                
                # Clean up the captured segment: strip trailing comments and statements that shouldn't be there
                # T-SQL doesn't require semicolons, so we need to remove code that comes after the INTO statement
                # Look for patterns like /* comment */ or SELECT/INSERT/UPDATE after the main statement
                raw_seg_cleaned = raw_seg
                # Remove block comments followed by diagnostic SELECT @variable statements
                # The diagnostic code pattern: /* ... */ followed by SELECT @variable ... statements
                # Only remove if it's followed by diagnostic patterns, not other INTO or JOIN keywords
                cleaned = _re.sub(r'/\*.*?\*/\s*(?:SELECT\s+@\w+\s+.*?(?=;|GO|\Z)|PRINT\s+.*?(?=;|GO|\Z))', '', raw_seg_cleaned, flags=_re.MULTILINE | _re.DOTALL)
                if cleaned != raw_seg_cleaned:
                    raw_seg_cleaned = cleaned
                
                seg = self._preprocess_sql(self._normalize_tsql(raw_seg_cleaned))
                try:
                    import sqlglot
                    from sqlglot import expressions as _exp
                    st = sqlglot.parse_one(seg, read=self.dialect)
                    if isinstance(st, _exp.Select):
                        # registers temp_registry/temp_sources/temp_lineage
                        self._parse_select_into(st, object_hint)
                except Exception as ast_err:
                    # Fallback: approximate base deps from string scan
                    try:
                        if tmp:
                            tkey = f"#{tmp}"
                            bases = self._extract_basic_dependencies(raw_seg) or set()
                            # Filter out self and temps
                            bases = {b for b in bases if '#' not in b and 'tempdb' not in str(b).lower()}
                            if bases:
                                # Update temp_sources (merge with existing if any)
                                existing = self.temp_sources.get(tkey, set())
                                existing.update(bases)
                                self.temp_sources[tkey] = existing
                                # Also register temp in temp_registry if not already there (for columns)
                                if tkey not in self.temp_registry:
                                    # Try to extract column names from SELECT
                                    import re as _re2
                                    select_match = _re2.search(r'(?is)SELECT\s+(.*?)\s+INTO', raw_seg)
                                    if select_match:
                                        select_list = select_match.group(1)
                                        # Simple column extraction - take aliases or column names
                                        cols = []
                                        for col_match in _re2.finditer(r'(\w+)(?:\s+AS\s+(\w+))?', select_list):
                                            alias = col_match.group(2) or col_match.group(1)
                                            cols.append(alias)
                                        if cols:
                                            self.temp_registry[tkey] = cols
                    except Exception as e:
                        import traceback
                        logger.debug(f"Error in regex fallback for {tmp}: {e}")
                        traceback.print_exc()
                        pass
            # INSERT INTO #temp SELECT ... segments
            for m in _re.finditer(r"(?is)\bINSERT\s+INTO\s+#(?P<tmp>[A-Za-z0-9_]+)\b.*?\bSELECT\b.*?(?=;|\bINSERT\b|\bCREATE\b|\bALTER\b|\bUPDATE\b|\bDELETE\b|\bEND\b|\bGO\b|$)", src_text):
                raw_seg = m.group(0)
                seg = self._preprocess_sql(self._normalize_tsql(raw_seg))
                try:
                    import sqlglot
                    from sqlglot import expressions as _exp
                    st = sqlglot.parse_one(seg, read=self.dialect)
                    if isinstance(st, _exp.Insert):
                        self._parse_insert_select(st, object_hint)
                except Exception:
                    # No AST; try to at least approximate deps for the temp
                    try:
                        tmp = m.group('tmp')
                        if tmp:
                            tkey = f"#{tmp}"
                            bases = self._extract_basic_dependencies(raw_seg) or set()
                            bases = {b for b in bases if '#' not in b and 'tempdb' not in str(b).lower()}
                            if bases:
                                # Update temp_sources (merge with existing if any)
                                existing = self.temp_sources.get(tkey, set())
                                existing.update(bases)
                                self.temp_sources[tkey] = existing
                                # Also register temp in temp_registry if not already there
                                if tkey not in self.temp_registry:
                                    # Try to extract column names from INSERT INTO ... SELECT
                                    import re as _re2
                                    insert_match = _re2.search(r'(?is)INSERT\s+INTO\s+#\w+.*?SELECT\s+(.*?)(?:\s+FROM|\s+WHERE|\s+GROUP|\s+ORDER|$)', raw_seg)
                                    if insert_match:
                                        select_list = insert_match.group(1)
                                        cols = []
                                        for col_match in _re2.finditer(r'(\w+)(?:\s+AS\s+(\w+))?', select_list):
                                            alias = col_match.group(2) or col_match.group(1)
                                            cols.append(alias)
                                        if cols:
                                            self.temp_registry[tkey] = cols
                    except Exception as e:
                        logger.debug(f"Error in INSERT fallback for {tmp}: {e}")
                        pass
        except Exception:
            pass

        # Expand temp dependencies on the final output, if any
        # Keep temp tables in dependencies AND expand them to base sources
        try:
            deps_expanded = set(materialized_output.dependencies or [])
            # Collect all temp tables found in dependencies
            temp_tables_found = set()
            # String-derived temp base map: #temp -> base deps
            temp_base_map: dict[str, Set[str]] = {}
            try:
                import re as _re
                for m2 in _re.finditer(r"(?is)\bSELECT\s+.*?\bINTO\s+#(?P<tmp>[A-Za-z0-9_]+)\b.*?(?=;|\bINSERT\b|\bCREATE\b|\bALTER\b|\bUPDATE\b|\bDELETE\b|\bEND\b|\bGO\b|$)", src_text):
                    raw_seg2 = m2.group(0)
                    tname2 = m2.group('tmp')
                    if tname2:
                        tkey = f"#{tname2}"
                        temp_tables_found.add(tkey)
                        bases2 = self._extract_basic_dependencies(raw_seg2) or set()
                        temp_base_map[tkey] = {b for b in bases2 if '#' not in b and 'tempdb' not in str(b).lower()}
            except Exception:
                pass
            # Also collect temp tables from INSERT INTO #temp
            try:
                import re as _re
                for m3 in _re.finditer(r"(?is)\bINSERT\s+INTO\s+#(?P<tmp>[A-Za-z0-9_]+)\b.*?(?=;|\bINSERT\b|\bCREATE\b|\bALTER\b|\bUPDATE\b|\bDELETE\b|\bEND\b|\bGO\b|$)", src_text):
                    tname3 = m3.group('tmp')
                    if tname3:
                        tkey = f"#{tname3}"
                        temp_tables_found.add(tkey)
            except Exception:
                pass
            for d in list(deps_expanded):
                low = str(d).lower()
                is_temp = ('#' in d) or ('tempdb' in low)
                if not is_temp:
                    simple = d.split('.')[-1]
                    tkey = f"#{simple}"
                    if tkey not in self.temp_sources and tkey not in temp_base_map:
                        continue
                else:
                    if '#' in d:
                        tname = d.split('#', 1)[1]
                        tname = tname.split('.')[0]
                        tkey = f"#{tname}"
                    else:
                        simple = d.split('.')[-1]
                        tkey = f"#{simple}"
                # Keep the temp table in dependencies
                temp_tables_found.add(tkey)
                bases = set(self.temp_sources.get(tkey, set()))
                if not bases and tkey in temp_base_map:
                    bases = set(temp_base_map[tkey])
                if bases:
                    deps_expanded.update(bases)
            # Add all temp tables found to dependencies (with canonical names)
            for tkey in temp_tables_found:
                # Use canonical temp table name if available
                ctx_db = getattr(self, '_ctx_db', None) or self.current_database or self.default_database
                ctx_obj = getattr(self, '_ctx_obj', None) or procedure_name
                if ctx_db and ctx_obj:
                    # Normalize procedure name for canonical temp naming
                    norm_proc = self._normalize_table_name_for_output(ctx_obj) if hasattr(self, '_normalize_table_name_for_output') else ctx_obj.split('.')[-1]
                    canonical_name = f"{ctx_db}.dbo.{norm_proc}#{tkey.lstrip('#')}"
                    deps_expanded.add(canonical_name)
                else:
                    deps_expanded.add(tkey)
            materialized_output.dependencies = deps_expanded
        except Exception:
            pass

        # Last-resort: if deps still show only temp table(s), broaden using basic scan
        try:
            deps_now = set(materialized_output.dependencies or [])
            looks_like_only_temp = False
            if deps_now and all(('#' not in d and d.split('.')[-1].startswith('SRC_')) or ('#' in d) or ('tempdb' in str(d).lower()) for d in deps_now):
                looks_like_only_temp = True
            if not deps_now or looks_like_only_temp:
                broad = self._extract_basic_dependencies(sql_content) or set()
                if broad:
                    # Filter out bogus tokens (e.g., stray '=')
                    filt = {b for b in broad if re.match(r'^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$', str(b))}
                    materialized_output.dependencies = filt or broad
        except Exception:
            pass

        # Learn from procedure CREATE only if raw name had explicit DB
        try:
            m = re.search(r'(?is)\bCREATE\s+(?:PROC|PROCEDURE)\s+([^\s(]+)', sql_content)
            raw_ident = m.group(1) if m else ""
            db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
            if self.registry and db_raw:
                self.registry.learn_from_create("procedure", f"{sch_raw}.{tbl_raw}", db_raw)
        except Exception:
            pass
        # Normalize output name for grouping (schema.table)
        try:
            if getattr(materialized_output, 'schema', None) and getattr(materialized_output.schema, 'name', None):
                norm_name = self._normalize_table_name_for_output(materialized_output.schema.name)
                materialized_output.schema.name = norm_name
                materialized_output.name = norm_name
        except Exception:
            pass
        # DON'T restore context before returning - it's needed for temp table canonical naming in engine.py
        # The context will be restored when the next file is parsed (in parse_sql_file)
        # IMPORTANT: Ensure context is set before returning, so engine.py can use it for canonical temp naming
        # Context should already be set on lines 35-36, but ensure it's set here as well
        # Force set context to ensure it's available after return
        self._ctx_db = inferred_db or self.current_database or self.default_database
        self._ctx_obj = self._normalize_table_name_for_output(procedure_name)
        # Debug: print context before returning (for immediate visibility)
        logger.debug(f"_parse_procedure_string: Setting context before return: _ctx_db={self._ctx_db}, _ctx_obj={self._ctx_obj}, procedure_name={procedure_name}")
        # self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
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
        self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
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
        self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
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
        self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
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
    # restore context before returning
    self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
    return obj


def _extract_procedure_name(self, sql_content: str) -> Optional[str]:
    """Extract procedure name from CREATE PROCEDURE statement (string)."""
    match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+([^\s\(]+)', sql_content, re.IGNORECASE)
    return match.group(1).strip() if match else None


def _extract_procedure_body(self, sql_content: str) -> Optional[str]:
    """Extract the body of a CREATE PROCEDURE (everything after AS keyword)."""
    # Match CREATE PROCEDURE ... AS and extract everything after
    match = re.search(
        r'(?is)CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+\S+.*?\bAS\b\s*(.*)',
        sql_content,
        re.DOTALL
    )
    if match:
        return match.group(1)
    return None


def _parse_procedure_body_statements(self, body_sql: str, object_hint: Optional[str] = None, full_sql: str = "") -> ObjectInfo:
    """
    Parse procedure body statements directly (fallback when CREATE PROCEDURE fails in sqlglot).
    Extracts INSERT INTO ... SELECT statements and builds lineage.
    """
    logger.debug(f"_parse_procedure_body_statements: Called with object_hint={object_hint}")
    from ..openlineage_utils import sanitize_name
    
    procedure_name = self._extract_procedure_name(full_sql) or object_hint or "unknown_procedure"
    
    # Infer DB and namespace
    inferred_db = self._infer_database_for_object(statement=None, sql_text=full_sql) or self.current_database or self.default_database
    namespace = self._canonical_namespace(inferred_db)
    
    # Set context
    prev_ctx_db, prev_ctx_obj = getattr(self, "_ctx_db", None), getattr(self, "_ctx_obj", None)
    try:
        self._ctx_db = (namespace.rsplit('/', 1)[-1]) if isinstance(namespace, str) else (self.current_database or self.default_database)
    except Exception:
        self._ctx_db = (self.current_database or self.default_database)
    # Normalize procedure_name to ensure it's in schema.table format (without brackets)
    self._ctx_obj = self._normalize_table_name_for_output(procedure_name)
    logger.debug(f"_parse_procedure_body_statements: Set context: _ctx_db={self._ctx_db}, _ctx_obj={self._ctx_obj}, procedure_name={procedure_name}")
    
    # Before preprocessing, find all CTEs that are used before SELECT ... INTO statements
    # This is needed because preprocessing may remove WITH clauses
    # Find all "SELECT ... INTO #table" patterns in body_sql
    into_temp_pattern = r'SELECT\s+.*?\bINTO\s+(#\w+)'
    into_matches_in_body = list(re.finditer(into_temp_pattern, body_sql, re.IGNORECASE | re.DOTALL))
    
    # For each SELECT ... INTO, find CTEs before it
    for into_match in into_matches_in_body:
        temp_table = into_match.group(1)
        # Find the position of "INTO" keyword within the match (not SELECT)
        match_text = body_sql[into_match.start():into_match.end()]
        into_keyword_offset = match_text.upper().find('INTO')
        if into_keyword_offset < 0:
            continue  # Skip if INTO not found (should not happen with correct regex)
        into_pos = into_match.start() + into_keyword_offset
        
        # Look backwards for WITH statements before this INTO (up to 50000 chars to handle large procedures)
        search_start = max(0, into_pos - 50000)
        before_into_text = body_sql[search_start:into_pos]
        
        # Find the WITH statement that DIRECTLY precedes this INTO
        # Strategy: find the CLOSEST WITH before our target INTO
        # Find all WITH statements in the window before this INTO
        all_with_matches = list(re.finditer(r'(?:^|;|\n)\s*WITH\s+', before_into_text, re.IGNORECASE | re.MULTILINE))
        
        # The closest WITH is simply the LAST one in before_into_text
        # (since before_into_text ends right before our SELECT INTO)
        closest_with = all_with_matches[-1] if all_with_matches else None
        
        if closest_with:
            # Find the actual "WITH" keyword position (skip semicolon/newline prefix in regex match)
            match_text = closest_with.group(0)
            with_keyword_offset = match_text.upper().find('WITH')
            with_start = closest_with.start() + with_keyword_offset
            with_block = before_into_text[with_start:]
            
            # Debug: check if all expected CTEs are in the block
            # Find ALL CTE definitions in this WITH block: name AS (...)
            # This pattern matches both "WITH name AS (" and ", name AS ("
            cte_pattern = r'(\w+)\s+AS\s*\('
            cte_matches = list(re.finditer(cte_pattern, with_block, re.IGNORECASE))
            
            for cte_idx, cte_match in enumerate(cte_matches):
                cte_name = cte_match.group(1)
                cte_start_in_block = cte_match.start()
                cte_start_absolute = search_start + with_start + cte_start_in_block
                
                # Find the matching closing paren for this CTE
                # Start from the opening paren after "AS ("
                paren_start = cte_match.end() - 1  # Position of opening paren in with_block
                paren_count = 0
                paren_pos = paren_start
                cte_end_pos = None
                
                # Count parentheses to find the matching closing paren
                while paren_pos < len(with_block):
                    if with_block[paren_pos] == '(':
                        paren_count += 1
                    elif with_block[paren_pos] == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            cte_end_pos = search_start + with_start + paren_pos + 1
                            break
                    paren_pos += 1
            
                if cte_end_pos:
                    # Extract the CTE definition
                    # For first CTE, include WITH keyword; for others, construct WITH manually
                    if cte_idx == 0:
                        # First CTE: extract from "WITH name AS (...)" 
                        cte_def_start = search_start + with_start
                        cte_sql = body_sql[cte_def_start:cte_end_pos]
                        cte_sql = cte_sql.lstrip('; \n\r\t')
                    else:
                        # Subsequent CTE: extract just "name AS (...)" and prepend WITH
                        cte_sql = "WITH " + body_sql[cte_start_absolute:cte_end_pos].lstrip(', \n\r\t')
                    
                    logger.debug(f"_parse_procedure_body_statements: Found CTE {cte_name} (#{cte_idx+1}) before INTO {temp_table}")
                    
                    # Try to parse and register the CTE
                    try:
                        # Parse the CTE definition (WITH ... AS (...))
                        # Add a dummy SELECT to make it valid SQL
                        cte_parse_sql = cte_sql + " SELECT 1"
                        parsed_cte = sqlglot.parse_one(cte_parse_sql, read=self.dialect)
                        
                        if isinstance(parsed_cte, exp.With) and hasattr(parsed_cte, 'expressions'):
                            # Extract CTE from exp.With
                            for cte_expr in parsed_cte.expressions:
                                if hasattr(cte_expr, 'alias') and str(cte_expr.alias).upper() == cte_name.upper():
                                    # Register the CTE
                                    if isinstance(cte_expr.this, exp.Select):
                                        self._process_ctes(parsed_cte.this if isinstance(parsed_cte.this, exp.Select) else cte_expr.this)
                                        logger.debug(f"_parse_procedure_body_statements: Registered CTE {cte_name}, cte_registry now has: {list(self.cte_registry.keys())}")
                                    break
                        elif isinstance(parsed_cte, exp.Select):
                            # If parsed as SELECT, try to extract columns
                            cte_columns = []
                            for proj in parsed_cte.expressions:
                                col_name = None
                                if isinstance(proj, exp.Alias):
                                    col_name = str(proj.alias) if hasattr(proj, 'alias') and proj.alias else None
                                    if not col_name and hasattr(proj, 'alias_or_name'):
                                        col_name = str(proj.alias_or_name)
                                elif isinstance(proj, exp.Column):
                                    col_name = str(proj.this) if hasattr(proj, 'this') else str(proj)
                                if col_name:
                                    cte_columns.append(col_name)
                            # Register CTE
                            self.cte_registry[cte_name] = {
                                'columns': cte_columns,
                                'definition': parsed_cte
                            }
                            logger.debug(f"_parse_procedure_body_statements: Registered CTE {cte_name} (as SELECT), cte_registry now has: {list(self.cte_registry.keys())}")
                    except Exception as e:
                        logger.debug(f"_parse_procedure_body_statements: Failed to parse CTE {cte_name}: {e}")
                        import traceback
                        logger.debug(f"_parse_procedure_body_statements: Traceback: {traceback.format_exc()}")
    
    # Preprocess body (remove DECLARE, SET, etc.)
    preprocessed_body = self._preprocess_sql(body_sql)
    logger.debug(f"_parse_procedure_body_statements: preprocessed_body length={len(preprocessed_body)}")
    # Check if SELECT INTO is in preprocessed_body
    if 'INTO #' in preprocessed_body or 'INTO #asefl_temp' in preprocessed_body:
        logger.debug(f"_parse_procedure_body_statements: Found 'INTO #' in preprocessed_body")
    if 'WITH' in preprocessed_body.upper():
        logger.debug(f"_parse_procedure_body_statements: Found 'WITH' in preprocessed_body")
    
    # Register temp tables found in procedure body for proper namespace resolution
    # sqlglot drops '#' from temp table names, so we need to register them beforehand
    temp_pattern = r'#(\w+)'
    temp_tables_found = []
    for match in re.finditer(temp_pattern, preprocessed_body):
        temp_name = f"#{match.group(1)}"
        if temp_name not in self.temp_registry:
            # Register with placeholder schema (will be filled during actual INSERT parsing)
            self.temp_registry[temp_name] = []
            temp_tables_found.append(temp_name)
    if temp_tables_found:
        logger.debug(f"_parse_procedure_body_statements: Found temp tables: {temp_tables_found}")
    
    # Initialize outputs and inputs before parsing
    all_outputs = []
    all_inputs: Set[str] = set()
    logger.debug(f"_parse_procedure_body_statements START: object_hint={object_hint}")
    
    # Parse statements in body - use one_statement mode to be more forgiving
    try:
        statements = []
        # sqlglot and exp are already imported at module level
        
        # First, try to parse the entire preprocessed_body to find WITH ... SELECT ... INTO in AST
        # This is more reliable than regex extraction
        try:
            all_statements = sqlglot.parse(preprocessed_body, read=self.dialect) or []
            logger.debug(f"_parse_procedure_body_statements: Parsed {len(all_statements)} statements from preprocessed_body")
            
            # Walk through AST to find WITH ... SELECT ... INTO
            for stmt in all_statements:
                # Check if this is a WITH statement containing SELECT ... INTO
                if isinstance(stmt, exp.With) and hasattr(stmt, 'this'):
                    if isinstance(stmt.this, exp.Select) and self._is_select_into(stmt.this):
                        logger.debug(f"_parse_procedure_body_statements: Found WITH ... SELECT ... INTO in AST")
                        statements.append(stmt)
                        # Also process it directly
                        obj = self._parse_select_into(stmt.this, object_hint)
                        if obj:
                            all_outputs.append(obj)
                            all_inputs.update(obj.dependencies or [])
                    elif isinstance(stmt.this, exp.Select) and not self._is_select_into(stmt.this):
                        # This is a standalone WITH statement (CTE definition) - process CTEs and register them
                        # This handles cases like "WITH MaxDates AS ... SELECT ... INTO #MaxLoadDate FROM MaxDates"
                        # where WITH is a separate statement before SELECT INTO
                        logger.debug(f"_parse_procedure_body_statements: Found standalone WITH statement (CTE definition), processing CTEs")
                        self._process_ctes(stmt)
                        logger.debug(f"_parse_procedure_body_statements: Processed CTEs from standalone WITH, cte_registry keys: {list(self.cte_registry.keys())}")
                # Check if this is a SELECT ... INTO (without WITH)
                elif isinstance(stmt, exp.Select) and self._is_select_into(stmt):
                    logger.debug(f"_parse_procedure_body_statements: Found SELECT ... INTO in AST")
                    statements.append(stmt)
                    obj = self._parse_select_into(stmt, object_hint)
                    if obj:
                        all_outputs.append(obj)
                        all_inputs.update(obj.dependencies or [])
        except Exception as e:
            logger.debug(f"_parse_procedure_body_statements: Failed to parse preprocessed_body as whole: {e}")
            # Fall back to regex-based extraction
            pass
        
        # Before fallback extraction, try to find and parse standalone WITH statements
        # This handles cases like "WITH MaxDates AS ... SELECT ... INTO #MaxLoadDate FROM MaxDates"
        # where WITH is a separate statement before SELECT INTO
        try:
            # Find all "SELECT ... INTO #table" patterns first
            into_temp_pattern = r'INTO\s+(#\w+)'
            into_matches = list(re.finditer(into_temp_pattern, preprocessed_body, re.IGNORECASE))
            
            # For each INTO, look backwards for standalone WITH statements
            for into_match in into_matches:
                temp_table = into_match.group(1)
                into_start = into_match.start()
                # Look backwards for WITH statements (up to 10000 chars)
                search_start = max(0, into_start - 10000)
                before_text = preprocessed_body[search_start:into_start]
                
                # Find all WITH statements before this INTO
                with_pattern = r'(?:^|;|\n)\s*WITH\s+(\w+)\s+AS\s*\([^)]*(?:\([^)]*\)[^)]*)*\)'
                with_matches = list(re.finditer(with_pattern, before_text, re.IGNORECASE | re.MULTILINE | re.DOTALL))
                
                # Also try a simpler pattern that matches WITH ... AS (...) where the closing paren is balanced
                if not with_matches:
                    # Find WITH statements by looking for "WITH name AS (" and finding the matching closing paren
                    simple_with_pattern = r'(?:^|;|\n)\s*WITH\s+(\w+)\s+AS\s*\('
                    simple_with_matches = list(re.finditer(simple_with_pattern, before_text, re.IGNORECASE | re.MULTILINE))
                    for simple_match in simple_with_matches:
                        # Find the matching closing paren
                        cte_name = simple_match.group(1)
                        open_pos = simple_match.end() - 1  # Position of opening paren
                        paren_count = 0
                        found_close = False
                        for i in range(open_pos, len(before_text)):
                            if before_text[i] == '(':
                                paren_count += 1
                            elif before_text[i] == ')':
                                paren_count -= 1
                                if paren_count == 0:
                                    # Found matching closing paren
                                    with_start = simple_match.start() + search_start
                                    with_end = i + search_start + 1
                                    with_stmt_text = preprocessed_body[with_start:with_end].strip()
                                    # Check if this WITH statement is followed by SELECT (not SELECT INTO)
                                    after_with = preprocessed_body[with_end:into_start].strip()
                                    if re.match(r'^\s*SELECT\s+', after_with, re.IGNORECASE) and 'INTO' not in after_with[:200]:
                                        # This might be a standalone WITH statement
                                        try:
                                            parsed_with = sqlglot.parse_one(with_stmt_text, read=self.dialect)
                                            if isinstance(parsed_with, exp.With):
                                                if hasattr(parsed_with, 'this') and isinstance(parsed_with.this, exp.Select):
                                                    if not self._is_select_into(parsed_with.this):
                                                        # This is a standalone WITH statement (CTE definition)
                                                        logger.debug(f"_parse_procedure_body_statements: Found standalone WITH statement {cte_name} before INTO {temp_table} in fallback, processing CTEs")
                                                        self._process_ctes(parsed_with.this)
                                                        logger.debug(f"_parse_procedure_body_statements: Processed CTEs from standalone WITH {cte_name} in fallback, cte_registry keys: {list(self.cte_registry.keys())}")
                                                        break  # Process only the first/last WITH before this INTO
                                        except Exception as parse_error:
                                            logger.debug(f"_parse_procedure_body_statements: Failed to parse standalone WITH statement {cte_name} before INTO {temp_table}: {parse_error}")
                                    found_close = True
                                    break
                        if found_close:
                            break
        except Exception as fallback_error:
            logger.debug(f"_parse_procedure_body_statements: Failed to process standalone WITH statements in fallback: {fallback_error}")
        
        # Fallback: try to extract WITH ... SELECT ... INTO using regex
        # Strategy: Find all "INTO #table" patterns, then look backwards for the nearest "WITH"
        into_temp_pattern = r'INTO\s+(#\w+)'
        into_matches = list(re.finditer(into_temp_pattern, preprocessed_body, re.IGNORECASE))
        with_select_into_matches = []
        select_into_without_with_matches = []  # For SELECT ... INTO without WITH
        
        for into_match in into_matches:
            temp_table = into_match.group(1)
            into_end = into_match.end()
            # Look backwards for the start of this statement (WITH or SELECT)
            # Search up to 10000 characters back (WITH statements can be very long)
            search_start = max(0, into_match.start() - 10000)
            before_text = preprocessed_body[search_start:into_match.start()]
            # IMPORTANT: local_search_start is used to track the actual search position
            # in before_text for calculating positions in preprocessed_body
            # It may be different from search_start if we extend the search range
            local_search_start = search_start
            
            logger.debug(f"_parse_procedure_body_statements: Looking for WITH before INTO {temp_table}, searching {len(before_text)} chars back, search_start={search_start}, into_match.start()={into_match.start()}")
            # Check if WITH is in preprocessed_body before into_match.start()
            if into_match.start() > 0:
                preprocessed_before_into = preprocessed_body[:into_match.start()]
                has_with_in_preprocessed = 'WITH' in preprocessed_before_into.upper() or 'WITH' in preprocessed_before_into
                logger.debug(f"_parse_procedure_body_statements: has_with_in_preprocessed={has_with_in_preprocessed} for {temp_table}, preprocessed_before_into length={len(preprocessed_before_into)}")
                if not has_with_in_preprocessed:
                    # Show first 500 chars of preprocessed_body to see what's at the start
                    logger.debug(f"_parse_procedure_body_statements: First 500 chars of preprocessed_body: {preprocessed_body[:500]}")
                    # Check if WITH is in entire preprocessed_body (maybe it's after into_match.start())
                    has_with_anywhere = 'WITH' in preprocessed_body.upper() or 'WITH' in preprocessed_body
                    logger.debug(f"_parse_procedure_body_statements: has_with_anywhere={has_with_anywhere} in entire preprocessed_body (length={len(preprocessed_body)})")
                    if has_with_anywhere:
                        # Find all WITH positions in preprocessed_body
                        with_positions = []
                        search_pos = 0
                        while True:
                            pos = preprocessed_body.upper().find('WITH', search_pos)
                            if pos < 0:
                                break
                            with_positions.append(pos)
                            search_pos = pos + 1
                        logger.debug(f"_parse_procedure_body_statements: WITH found at positions {with_positions} in preprocessed_body")
                        # Show context around first WITH
                        if with_positions:
                            first_with_pos = with_positions[0]
                            context_start = max(0, first_with_pos - 50)
                            context_end = min(len(preprocessed_body), first_with_pos + 200)
                            logger.debug(f"_parse_procedure_body_statements: Context around first WITH in preprocessed_body: {preprocessed_body[context_start:context_end]}")
                else:
                    # Find WITH position in preprocessed_body
                    with_pos = preprocessed_before_into.upper().find('WITH')
                    if with_pos >= 0:
                        logger.debug(f"_parse_procedure_body_statements: WITH found at position {with_pos} in preprocessed_body (before INTO {temp_table})")
                        # Show context around WITH
                        context_start = max(0, with_pos - 50)
                        context_end = min(len(preprocessed_before_into), with_pos + 200)
                        logger.debug(f"_parse_procedure_body_statements: Context around WITH in preprocessed_body: {preprocessed_before_into[context_start:context_end]}")
            # Check if WITH is in before_text
            if 'WITH' in before_text.upper() or 'WITH' in before_text:
                logger.debug(f"_parse_procedure_body_statements: Found 'WITH' text in before_text for {temp_table}")
                # Show first 200 chars to see if WITH is at the start
                logger.debug(f"_parse_procedure_body_statements: First 200 chars of before_text: {before_text[:200]}")
                # Show last 200 chars of before_text
                logger.debug(f"_parse_procedure_body_statements: Last 200 chars before INTO {temp_table}: {before_text[-200:]}")
            else:
                logger.debug(f"_parse_procedure_body_statements: No 'WITH' text found in before_text for {temp_table}")
                # Show first 200 chars to see what's at the start
                logger.debug(f"_parse_procedure_body_statements: First 200 chars of before_text: {before_text[:200]}")
                # Show last 200 chars of before_text to see what's there
                logger.debug(f"_parse_procedure_body_statements: Last 200 chars before INTO {temp_table}: {before_text[-200:]}")
                # Also check if WITH is in preprocessed_body before into_match.start() (maybe search_start is wrong)
                # Always check extended range if WITH is not found in before_text
                # NOTE: Use local variables to avoid modifying search_start which affects later iterations
                extended_search_start = max(0, into_match.start() - 20000)
                extended_before_text = preprocessed_body[extended_search_start:into_match.start()]
                logger.debug(f"_parse_procedure_body_statements: Checking extended_before_text for {temp_table}, length={len(extended_before_text)}, extended_search_start={extended_search_start}, into_match.start()={into_match.start()}")
                # Check if WITH is in extended_before_text
                has_with_in_extended = 'WITH' in extended_before_text.upper() or 'WITH' in extended_before_text
                logger.debug(f"_parse_procedure_body_statements: has_with_in_extended={has_with_in_extended} for {temp_table}")
                if has_with_in_extended:
                    logger.debug(f"_parse_procedure_body_statements: Found 'WITH' in extended_before_text (extended_search_start={extended_search_start}), but not in before_text (search_start={search_start})")
                    # Find WITH in extended_before_text
                    with_pos_in_extended = extended_before_text.upper().find('WITH')
                    if with_pos_in_extended >= 0:
                        logger.debug(f"_parse_procedure_body_statements: WITH found at position {extended_search_start + with_pos_in_extended} in preprocessed_body")
                        # Show context around WITH
                        context_start = max(0, with_pos_in_extended - 50)
                        context_end = min(len(extended_before_text), with_pos_in_extended + 200)
                        logger.debug(f"_parse_procedure_body_statements: Context around WITH: {extended_before_text[context_start:context_end]}")
                        # Use extended_before_text instead of before_text for finding WITH
                        # IMPORTANT: Create local variables to avoid modifying search_start for other iterations
                        before_text = extended_before_text
                        local_search_start = extended_search_start
                    else:
                        local_search_start = search_start
                else:
                    local_search_start = search_start
            
            # Find the nearest WITH statement before this INTO
            # IMPORTANT: Only match WITH if it's immediately followed by CTEs and then SELECT
            # This avoids matching WITH statements from earlier SELECT...INTO blocks
            with_start_pattern = r'(?:^|;|\n)\s*WITH\s+\w+\s*AS\s*\('
            with_start_matches = list(re.finditer(with_start_pattern, before_text, re.IGNORECASE | re.MULTILINE))
            
            # Also try a simpler pattern - just look for WITH (might be at start of line)
            if not with_start_matches:
                simple_with_pattern = r'\bWITH\s+'
                simple_matches = list(re.finditer(simple_with_pattern, before_text, re.IGNORECASE))
                logger.debug(f"_parse_procedure_body_statements: Simple pattern found {len(simple_matches)} WITH matches in before_text for {temp_table}")
                # Filter to only those that are at start of statement (after ; or at start of search area)
                for simple_match in simple_matches:
                    pos = simple_match.start()
                    prev_char = before_text[pos-1] if pos > 0 else None
                    logger.debug(f"_parse_procedure_body_statements: WITH match at pos {pos} in before_text, prev_char={repr(prev_char)}")
                    # Check if this WITH is at start of statement
                    if pos == 0 or (pos > 0 and before_text[pos-1] in (';', '\n')):
                        with_start_matches.append(simple_match)
                        logger.debug(f"_parse_procedure_body_statements: Found WITH at position {local_search_start + pos} using simple pattern")
                    else:
                        logger.debug(f"_parse_procedure_body_statements: WITH at pos {pos} is not at start of statement (prev_char={repr(prev_char)})")
            
            logger.debug(f"_parse_procedure_body_statements: Found {len(with_start_matches)} WITH matches before INTO {temp_table}")
            
            # IMPORTANT: Filter out WITH statements that are NOT for this INTO
            # If there's a semicolon between the WITH and INTO in preprocessed_body,
            # the WITH is for a different statement
            valid_with_matches = []
            for with_match in with_start_matches:
                with_pos_in_before = with_match.start()
                actual_with_pos = local_search_start + with_pos_in_before
                # Check if there's a semicolon between WITH and INTO in preprocessed_body
                between_text = preprocessed_body[actual_with_pos:into_match.start()]
                semicolon_count = between_text.count(';')
                if semicolon_count == 0:
                    # No semicolon - this WITH is likely for this INTO
                    valid_with_matches.append(with_match)
                    logger.debug(f"_parse_procedure_body_statements: WITH at {actual_with_pos} is valid for {temp_table} (no semicolon between)")
                else:
                    logger.debug(f"_parse_procedure_body_statements: WITH at {actual_with_pos} is NOT valid for {temp_table} ({semicolon_count} semicolons between)")
            
            with_start_matches = valid_with_matches
            logger.debug(f"_parse_procedure_body_statements: After filtering, {len(with_start_matches)} valid WITH matches for {temp_table}")
            
            if not with_start_matches:
                # No WITH found - this is a simple SELECT ... INTO #table (without WITH)
                # Extract the SELECT statement that contains this INTO
                # Look backwards for SELECT
                select_start_pattern = r'(?:^|;|\n)\s*SELECT\s+'
                select_start_matches = list(re.finditer(select_start_pattern, before_text, re.IGNORECASE | re.MULTILINE))
                if select_start_matches:
                    # Use the last (closest) SELECT match
                    select_start_match = select_start_matches[-1]
                    select_pos_in_before = select_start_match.start()
                    actual_select_start = local_search_start + select_pos_in_before
                    # Adjust if it starts with semicolon or newline
                    while actual_select_start < len(preprocessed_body) and preprocessed_body[actual_select_start] in (';', '\n', '\r', ' '):
                        actual_select_start += 1
                    
                    # Find the end of this statement
                    # Look for semicolon, next SELECT, or control flow keywords (IF, BEGIN, etc.)
                    search_end_pos = min(len(preprocessed_body), into_end + 20000)
                    after_text = preprocessed_body[into_end:search_end_pos]
                    
                    # Look for semicolon first
                    semicolon_pos = after_text.find(';')
                    if semicolon_pos >= 0:
                        # Check if semicolon is followed by SELECT or control flow
                        after_semicolon = after_text[semicolon_pos+1:semicolon_pos+100].strip()
                        if re.match(r'\s*(SELECT|IF|BEGIN|END|ELSE|WHILE|FOR)\s+', after_semicolon, re.IGNORECASE):
                            actual_end = into_end + semicolon_pos + 1
                        else:
                            # Semicolon might be inside statement, continue searching
                            semicolon_pos = -1
                    
                    if semicolon_pos < 0:
                        # Look for EXEC statement first (display insert status, audit, etc.)
                        exec_match = re.search(r'\n\s*EXEC\s+', after_text, re.IGNORECASE | re.MULTILINE)
                        # Look for next SELECT on a new line (but not SELECT @var assignments)
                        next_select_match = re.search(r'\n\s*SELECT\s+(?!@)', after_text, re.IGNORECASE | re.MULTILINE)
                        
                        # Use whichever comes first
                        candidates = []
                        if exec_match:
                            candidates.append(('EXEC', exec_match.start()))
                        if next_select_match:
                            candidates.append(('SELECT', next_select_match.start()))
                        
                        if candidates:
                            # Pick the earliest one
                            earliest_type, earliest_pos = min(candidates, key=lambda x: x[1])
                            actual_end = into_end + earliest_pos
                        else:
                            # Look for control flow keywords (IF, BEGIN, etc.) on new line
                            control_flow_match = re.search(r'\n\s*(IF|BEGIN|END|ELSE|WHILE|FOR)\s*\(', after_text, re.IGNORECASE | re.MULTILINE)
                            if control_flow_match:
                                actual_end = into_end + control_flow_match.start()
                            else:
                                # Use limit, but try to find end of FROM clause
                                # Look for WHERE, GROUP BY, ORDER BY, or end of statement
                                clause_match = re.search(r'\n\s*(WHERE|GROUP\s+BY|ORDER\s+BY|HAVING)\s+', after_text, re.IGNORECASE | re.MULTILINE)
                                if clause_match:
                                    # Find end of this clause
                                    clause_end = clause_match.end()
                                    # Look for next keyword after this clause
                                    next_keyword = re.search(r'\n\s*(SELECT|IF|BEGIN|END|ELSE|;)\s+', after_text[clause_end:], re.IGNORECASE | re.MULTILINE)
                                    if next_keyword:
                                        actual_end = into_end + clause_end + next_keyword.start()
                                    else:
                                        actual_end = min(len(preprocessed_body), into_end + 8000)
                                else:
                                    actual_end = min(len(preprocessed_body), into_end + 8000)
                    
                    # Extract the full statement
                    full_stmt = preprocessed_body[actual_select_start:actual_end].strip()
                    if full_stmt.endswith(';'):
                        full_stmt = full_stmt[:-1].strip()
                    
                    if full_stmt:
                        class MatchObj:
                            def __init__(self, start, end, group_text):
                                self._start = start
                                self._end = end
                                self._group_text = group_text
                            def start(self):
                                return self._start
                            def end(self):
                                return self._end
                            def group(self, n):
                                return self._group_text if n == 1 else None
                        
                        select_into_without_with_matches.append(MatchObj(actual_select_start, actual_end, full_stmt))
                        logger.debug(f"_parse_procedure_body_statements: Found SELECT ... INTO {temp_table} (without WITH) (start={actual_select_start}, end={actual_end}, length={len(full_stmt)})")
            
            if with_start_matches:
                # Use the last (closest) WITH match
                with_start_match = with_start_matches[-1]
                # Position in before_text
                with_pos_in_before = with_start_match.start()
                # Actual position in preprocessed_body
                actual_with_start = local_search_start + with_pos_in_before
                # Adjust if it starts with semicolon or newline
                while actual_with_start < len(preprocessed_body) and preprocessed_body[actual_with_start] in (';', '\n', '\r', ' '):
                    actual_with_start += 1
                
                # Check if this WITH statement is standalone (not part of WITH ... SELECT ... INTO)
                # Look for the end of the WITH statement (should end before SELECT ... INTO)
                # Try to parse the WITH statement separately to register CTEs
                # Find where the WITH statement ends (should be before the SELECT that contains INTO)
                select_before_into_pattern = r'(?:^|;|\n)\s*SELECT\s+'
                select_before_into_matches = list(re.finditer(select_before_into_pattern, preprocessed_body[actual_with_start:into_match.start()], re.IGNORECASE | re.MULTILINE))
                if select_before_into_matches:
                    # There's a SELECT between WITH and INTO - this might be a standalone WITH statement
                    # The WITH statement should end before this SELECT
                    select_before_into_match = select_before_into_matches[-1]
                    with_end_pos = actual_with_start + select_before_into_match.start()
                    # Extract the WITH statement
                    with_stmt_str = preprocessed_body[actual_with_start:with_end_pos].strip()
                    # Try to parse it as a standalone WITH statement
                    try:
                        with_stmt_parsed = sqlglot.parse_one(with_stmt_str, read=self.dialect)
                        if isinstance(with_stmt_parsed, exp.With):
                            # This is a standalone WITH statement - process CTEs
                            logger.debug(f"_parse_procedure_body_statements: Found standalone WITH statement before INTO {temp_table}, processing CTEs")
                            self._process_ctes(with_stmt_parsed.this if hasattr(with_stmt_parsed, 'this') and isinstance(with_stmt_parsed.this, exp.Select) else with_stmt_parsed)
                            logger.debug(f"_parse_procedure_body_statements: Processed CTEs from standalone WITH before INTO {temp_table}, cte_registry keys: {list(self.cte_registry.keys())}")
                    except Exception as e:
                        logger.debug(f"_parse_procedure_body_statements: Failed to parse standalone WITH statement before INTO {temp_table}: {e}")
                
                # Find the end of this statement - look for semicolon or next SELECT after INTO #table
                # For #asefl_temp, the statement ends after the FROM clause (no semicolon, next statement is SELECT)
                # Strategy: Find the end of the FROM clause by looking for the next SELECT on a new line
                # that appears after all JOINs are done
                search_end_pos = min(len(preprocessed_body), into_end + 20000)
                after_text = preprocessed_body[into_end:search_end_pos]
                
                logger.debug(f"_parse_procedure_body_statements: Looking for end of statement for {temp_table}, after_text length={len(after_text)}")
                logger.debug(f"_parse_procedure_body_statements: First 300 chars after INTO {temp_table}: {after_text[:300]}")
                
                # For #asefl_temp, the statement ends after the FROM clause (no semicolon, next statement is SELECT)
                # Strategy: Always look for next SELECT or EXEC on a new line first, as it's more reliable than semicolon
                # The semicolon might be inside the statement (e.g., after JOINs but before the actual end)
                # Ignore SELECT @var assignments using negative lookahead
                exec_match = re.search(r'\n\s*EXEC\s+', after_text, re.IGNORECASE | re.MULTILINE)
                next_select_match = re.search(r'\n\s*SELECT\s+(?!@)', after_text, re.IGNORECASE | re.MULTILINE)
                
                # Use whichever comes first (EXEC or SELECT)
                candidates = []
                if exec_match:
                    candidates.append(('EXEC', exec_match.start()))
                if next_select_match:
                    candidates.append(('SELECT', next_select_match.start()))
                
                if candidates:
                    earliest_type, earliest_pos = min(candidates, key=lambda x: x[1])
                    logger.debug(f"_parse_procedure_body_statements: Found {earliest_type} at position {earliest_pos} after INTO {temp_table}")
                    actual_end = into_end + earliest_pos
                    logger.debug(f"_parse_procedure_body_statements: Ending statement at position {actual_end} (before {earliest_type})")
                else:
                    # No SELECT found, try to find a semicolon
                    semicolon_pos = after_text.find(';')
                    if semicolon_pos >= 0:
                        logger.debug(f"_parse_procedure_body_statements: Found semicolon at position {semicolon_pos} after INTO {temp_table}")
                        # Check if this semicolon is actually the end of the statement
                        # Look at what comes after the semicolon
                        after_semicolon = after_text[semicolon_pos+1:semicolon_pos+100].strip()
                        logger.debug(f"_parse_procedure_body_statements: After semicolon: {after_semicolon[:50]}")
                        # If the next thing is SELECT on a new line, this is likely the end
                        if re.match(r'\s*SELECT\s+', after_semicolon, re.IGNORECASE):
                            logger.debug(f"_parse_procedure_body_statements: Semicolon is followed by SELECT, using it as end")
                            actual_end = into_end + semicolon_pos + 1
                        else:
                            # The semicolon might be inside the statement, use limit
                            logger.debug(f"_parse_procedure_body_statements: Semicolon is not followed by SELECT, might be inside statement, using limit")
                            actual_end = min(len(preprocessed_body), into_end + 8000)
                    else:
                        # No SELECT and no semicolon found, use limit
                        logger.debug(f"_parse_procedure_body_statements: No SELECT and no semicolon found after INTO {temp_table}, using limit")
                        actual_end = min(len(preprocessed_body), into_end + 8000)
                
                # Extract the full statement
                full_stmt = preprocessed_body[actual_with_start:actual_end].strip()
                # Remove trailing semicolon if present
                if full_stmt.endswith(';'):
                    full_stmt = full_stmt[:-1].strip()
                
                if full_stmt:
                    # Create a match-like object
                    class MatchObj:
                        def __init__(self, start, end, group_text):
                            self._start = start
                            self._end = end
                            self._group_text = group_text
                        def start(self):
                            return self._start
                        def end(self):
                            return self._end
                        def group(self, n):
                            return self._group_text if n == 1 else None
                    
                    with_select_into_matches.append(MatchObj(actual_with_start, actual_end, full_stmt))
                    logger.debug(f"_parse_procedure_body_statements: Found WITH ... SELECT ... INTO {temp_table} (start={actual_with_start}, end={actual_end}, length={len(full_stmt)})")
                    # Show first 100 chars of extracted statement
                    logger.debug(f"_parse_procedure_body_statements: First 100 chars: {full_stmt[:100]}")
                    # Show last 100 chars of extracted statement
                    logger.debug(f"_parse_procedure_body_statements: Last 100 chars: {full_stmt[-100:]}")
        if with_select_into_matches:
            logger.debug(f"_parse_procedure_body_statements: Found {len(with_select_into_matches)} WITH ... SELECT ... INTO statements")
            for match_idx, match in enumerate(with_select_into_matches):
                with_stmt = match.group(1).strip()
                # Remove leading semicolon if present
                if with_stmt.startswith(';'):
                    with_stmt = with_stmt[1:].strip()
                logger.debug(f"_parse_procedure_body_statements: Trying to parse WITH ... SELECT ... INTO #{match_idx+1} (length={len(with_stmt)})")
                try:
                    parsed = sqlglot.parse_one(with_stmt, read=self.dialect)
                    if parsed:
                        statements.append(parsed)
                        logger.debug(f"_parse_procedure_body_statements: Successfully parsed WITH ... SELECT ... INTO #{match_idx+1} as {type(parsed).__name__}")
                        # If parsed as Select, check if it has WITH clause
                        if isinstance(parsed, exp.Select):
                            # Check if this SELECT has a WITH clause - if so, process CTEs first
                            with_clause = parsed.args.get('with')
                            if with_clause:
                                logger.debug(f"_parse_procedure_body_statements: Parsed Select has WITH clause, processing CTEs first")
                                self._process_ctes(parsed)
                                logger.debug(f"_parse_procedure_body_statements: Processed CTEs from SELECT WITH, cte_registry keys: {list(self.cte_registry.keys())}")
                            if self._is_select_into(parsed):
                                logger.debug(f"_parse_procedure_body_statements: Parsed Select has INTO, processing it now...")
                                try:
                                    obj = self._parse_select_into(parsed, object_hint)
                                    if obj:
                                        all_outputs.append(obj)
                                        all_inputs.update(obj.dependencies or [])
                                        logger.debug(f"_parse_procedure_body_statements: Successfully processed SELECT INTO, got obj: {obj.name}")
                                except Exception as e:
                                    logger.debug(f"_parse_procedure_body_statements: Failed to process SELECT INTO: {e}")
                                    import traceback
                                    logger.debug(f"_parse_procedure_body_statements: Traceback: {traceback.format_exc()}")
                        elif isinstance(parsed, exp.With):
                            # If parsed as With, process CTEs first
                            if hasattr(parsed, 'this') and isinstance(parsed.this, exp.Select):
                                logger.debug(f"_parse_procedure_body_statements: Parsed With has SELECT, processing CTEs first")
                                self._process_ctes(parsed.this)
                                logger.debug(f"_parse_procedure_body_statements: Processed CTEs from WITH statement, cte_registry keys: {list(self.cte_registry.keys())}")
                                if self._is_select_into(parsed.this):
                                    logger.debug(f"_parse_procedure_body_statements: Parsed With has SELECT INTO, processing it now...")
                                    try:
                                        obj = self._parse_select_into(parsed.this, object_hint)
                                        if obj:
                                            all_outputs.append(obj)
                                            all_inputs.update(obj.dependencies or [])
                                            logger.debug(f"_parse_procedure_body_statements: Successfully processed SELECT INTO from With, got obj: {obj.name}")
                                    except Exception as e:
                                        logger.debug(f"_parse_procedure_body_statements: Failed to process SELECT INTO from With: {e}")
                                        import traceback
                                        logger.debug(f"_parse_procedure_body_statements: Traceback: {traceback.format_exc()}")
                except Exception as e:
                    logger.debug(f"_parse_procedure_body_statements: Failed to parse WITH ... SELECT ... INTO #{match_idx+1}: {e}")
                    # Fallback: try to register temp table with columns extracted from SQL string
                    try:
                        # Find the specific INTO #table in this match
                        temp_match = re.search(r'INTO\s+(#\w+)', match.group(1), re.IGNORECASE)
                        if temp_match:
                            temp_name = temp_match.group(1)
                            # Extract column names from SELECT clause - look for SELECT ... INTO #temp_name pattern
                            # Find the SELECT that immediately precedes this INTO #temp_name
                            # We need to find the SELECT that is closest to this INTO
                            into_pos = temp_match.start()
                            # Look backwards from INTO to find the SELECT
                            before_into = match.group(1)[:into_pos]
                            # Find the last SELECT before INTO
                            select_matches = list(re.finditer(r'(?is)\bSELECT\s+', before_into))
                            if select_matches:
                                # Use the last SELECT (closest to INTO)
                                last_select = select_matches[-1]
                                select_start = last_select.end()
                                # Extract everything from SELECT to INTO
                                select_to_into = match.group(1)[select_start:into_pos]
                                # Extract column names
                                col_names = []
                                for col_expr in re.split(r',\s*(?![^()]*\))', select_to_into):
                                    col_expr = col_expr.strip()
                                    # Skip if empty or too short
                                    if not col_expr or len(col_expr) < 2:
                                        continue
                                    # Remove AS alias if present
                                    if ' AS ' in col_expr.upper():
                                        col_expr = col_expr.rsplit(' AS ', 1)[-1].strip()
                                    elif ' ' in col_expr and not col_expr.startswith('['):
                                        # Might be alias without AS - take last word only if it's a simple identifier
                                        parts = col_expr.split()
                                        if len(parts) > 1:
                                            # Only use last part if it's a simple identifier (no special chars, no keywords)
                                            last_part = parts[-1].strip('[]').strip()
                                            if last_part and re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', last_part) and len(last_part) >= 2:
                                                col_expr = last_part
                                            else:
                                                # Skip this column - it's too complex
                                                continue
                                    # Remove brackets
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove SQL comments (-- and /* */)
                                    col_expr = re.sub(r'--.*$', '', col_expr, flags=re.MULTILINE).strip()
                                    col_expr = re.sub(r'/\*.*?\*/', '', col_expr, flags=re.DOTALL).strip()
                                    # Remove newlines and tabs - if there are newlines, it's probably not a column name
                                    if '\n' in col_expr or '\t' in col_expr or '\r' in col_expr:
                                        continue
                                    # Remove leading/trailing invalid characters
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove patterns like "abl.[column" -> "column"
                                    if '.' in col_expr and '[' in col_expr:
                                        parts = col_expr.split('.')
                                        col_expr = parts[-1].strip('[]').strip()
                                    # Validate: must be a valid column name (not SQL expression)
                                    # Must be a simple identifier (letters, numbers, underscore only)
                                    if col_expr and len(col_expr) >= 2 and len(col_expr) < 100 and \
                                       re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col_expr) and \
                                       not col_expr.upper().startswith('SELECT') and \
                                       not any(keyword in col_expr.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS', 'GROUP', 'BY', 'ORDER', 'HAVING']) and \
                                       col_expr not in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING'] and \
                                       not col_expr.startswith('(') and not col_expr.endswith(')') and \
                                       not col_expr.endswith('--') and not col_expr.startswith('['):
                                        col_names.append(col_expr)
                                # Register temp table with extracted columns
                                if col_names:
                                    self.temp_registry[temp_name] = col_names
                                    logger.debug(f"_parse_procedure_body_statements: Registered {temp_name} in temp_registry with {len(col_names)} columns from WITH ... SELECT ... INTO fallback: {col_names[:5]}")
                                
                                # Extract dependencies from FROM and JOIN clauses (regex-based fallback)
                                deps_from_sql: Set[str] = set()
                                try:
                                    sql_text = match.group(1)
                                    logger.debug(f"_parse_procedure_body_statements: Extracting dependencies for {temp_name}, sql_text length={len(sql_text)}, first 200 chars: {sql_text[:200][:200]}")

                                    def _clean_dep(dep: str) -> str:
                                        dep = dep.strip().rstrip(';,')
                                        dep = dep.replace('[', '').replace(']', '')
                                        return dep
                                    
                                    # Extract FROM clause table
                                    from_match = re.search(r'\bFROM\s+([\w.#@\[\]]+)', sql_text, re.IGNORECASE)
                                    if from_match:
                                        from_table = _clean_dep(from_match.group(1))
                                        # Simple table name extraction (will be qualified by _get_table_name during OpenLineage generation)
                                        if from_table and not from_table.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER']:
                                            deps_from_sql.add(from_table)
                                            logger.debug(f"_parse_procedure_body_statements: Found FROM table: {from_table} for {temp_name}")
                                    
                                    # Extract all JOIN clause tables (LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.)
                                    # Pattern: optional JOIN type + JOIN + table name
                                    for join_match in re.finditer(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+([#\w.\[\]]+)(?:\s+(?:AS\s+)?[#\w.\[\]]+)?(?=\s+ON|\s+WHERE|\s+,|\s+JOIN|\s+;|\s*$)', sql_text, re.IGNORECASE):
                                        join_table = _clean_dep(join_match.group(1))
                                        if join_table and not join_table.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER']:
                                            deps_from_sql.add(join_table)
                                            logger.debug(f"_parse_procedure_body_statements: Found JOIN table: {join_table} for {temp_name}")

                                    # Broaden with basic dependency finder to catch bracketed identifiers and aliases
                                    deps_from_sql.update({
                                        _clean_dep(dep) for dep in self._extract_basic_dependencies(sql_text)
                                    })
                                    
                                    # Store dependencies in temp_sources
                                    if not deps_from_sql:
                                        # Secondary scan around INTO in the full body to avoid truncated matches
                                        into_marker = f"INTO {temp_name}"
                                        pos = preprocessed_body.upper().find(into_marker.upper())
                                        if pos != -1:
                                            window = preprocessed_body[max(0, pos - 8000):pos + 8000]
                                            from_match = re.search(r'\bFROM\s+([\w.#@\[\]]+)', window, re.IGNORECASE)
                                            if from_match:
                                                deps_from_sql.add(_clean_dep(from_match.group(1)))
                                            for join_match in re.finditer(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+([#\w.\[\]]+)(?:\s+(?:AS\s+)?[#\w.\[\]]+)?(?=\s+ON|\s+WHERE|\s+,|\s+JOIN|\s+;|$)', window, re.IGNORECASE):
                                                jtbl = _clean_dep(join_match.group(1))
                                                if jtbl:
                                                    deps_from_sql.add(jtbl)
                                            deps_from_sql.update({
                                                _clean_dep(dep) for dep in self._extract_basic_dependencies(window)
                                            })
                                    if deps_from_sql:
                                        existing = set(self.temp_sources.get(temp_name, set()))
                                        existing.update(d for d in deps_from_sql if d)
                                        self.temp_sources[temp_name] = existing
                                        logger.debug(f"_parse_procedure_body_statements: Stored {len(deps_from_sql)} dependencies for {temp_name} in temp_sources")
                                except Exception as deps_error:
                                    logger.debug(f"_parse_procedure_body_statements: Failed to extract dependencies for {temp_name}: {deps_error}")


                    except Exception as fallback_error:
                        logger.debug(f"_parse_procedure_body_statements: Failed to register temp table from WITH ... SELECT ... INTO fallback: {fallback_error}")
        
        # Process SELECT ... INTO without WITH
        # Remove duplicates - same temp table might be found multiple times
        if select_into_without_with_matches:
            # Deduplicate by temp table name
            seen_tables = set()
            unique_matches = []
            for match in select_into_without_with_matches:
                # Extract temp table name from the statement
                temp_match = re.search(r'INTO\s+(#\w+)', match.group(1), re.IGNORECASE)
                if temp_match:
                    temp_name = temp_match.group(1)
                    if temp_name not in seen_tables:
                        seen_tables.add(temp_name)
                        unique_matches.append(match)
            select_into_without_with_matches = unique_matches
            
            logger.debug(f"_parse_procedure_body_statements: Found {len(select_into_without_with_matches)} SELECT ... INTO statements (without WITH) after deduplication")
            for match_idx, match in enumerate(select_into_without_with_matches):
                select_stmt = match.group(1).strip()
                if select_stmt.startswith(';'):
                    select_stmt = select_stmt[1:].strip()
                logger.debug(f"_parse_procedure_body_statements: Trying to parse SELECT ... INTO (without WITH) #{match_idx+1} (length={len(select_stmt)})")
                try:
                    parsed = sqlglot.parse_one(select_stmt, read=self.dialect)
                    if parsed:
                        statements.append(parsed)
                        logger.debug(f"_parse_procedure_body_statements: Successfully parsed SELECT ... INTO (without WITH) #{match_idx+1} as {type(parsed).__name__}")
                        if isinstance(parsed, exp.Select):
                            if self._is_select_into(parsed):
                                logger.debug(f"_parse_procedure_body_statements: Parsed Select has INTO, processing it now...")
                                try:
                                    obj = self._parse_select_into(parsed, object_hint)
                                    if obj:
                                        all_outputs.append(obj)
                                        all_inputs.update(obj.dependencies or [])
                                        logger.debug(f"_parse_procedure_body_statements: Successfully processed SELECT INTO (without WITH), got obj: {obj.name}")
                                except Exception as e:
                                    logger.debug(f"_parse_procedure_body_statements: Failed to process SELECT INTO (without WITH): {e}")
                                    import traceback
                                    logger.debug(f"_parse_procedure_body_statements: Traceback: {traceback.format_exc()}")
                except Exception as e:
                    logger.debug(f"_parse_procedure_body_statements: Failed to parse SELECT ... INTO (without WITH) #{match_idx+1}: {e}")
                    # Fallback: try to register temp table with columns extracted from SQL string
                    try:
                        temp_match = re.search(r'INTO\s+(#\w+)', match.group(1), re.IGNORECASE)
                        if temp_match:
                            temp_name = temp_match.group(1)
                            # Extract column names from SELECT clause using simple regex
                            select_part = match.group(1)
                            # Find SELECT ... INTO pattern
                            col_match = re.search(r'(?is)SELECT\s+(.*?)\s+INTO\s+', select_part)
                            if col_match:
                                col_list_str = col_match.group(1)
                                # Extract column names (simple approach - split by comma and take last part after AS or space)
                                col_names = []
                                for col_expr in re.split(r',\s*(?![^()]*\))', col_list_str):
                                    col_expr = col_expr.strip()
                                    # Skip if empty or too short
                                    if not col_expr or len(col_expr) < 2:
                                        continue
                                    # Remove AS alias if present
                                    if ' AS ' in col_expr.upper():
                                        col_expr = col_expr.rsplit(' AS ', 1)[-1].strip()
                                    elif ' ' in col_expr and not col_expr.startswith('['):
                                        # Might be alias without AS - take last word only if it's a simple identifier
                                        parts = col_expr.split()
                                        if len(parts) > 1:
                                            # Only use last part if it's a simple identifier (no special chars, no keywords)
                                            last_part = parts[-1].strip('[]').strip()
                                            if last_part and re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', last_part) and len(last_part) >= 2:
                                                col_expr = last_part
                                            else:
                                                # Skip this column - it's too complex
                                                continue
                                    # Remove brackets
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove SQL comments (-- and /* */)
                                    col_expr = re.sub(r'--.*$', '', col_expr, flags=re.MULTILINE).strip()
                                    col_expr = re.sub(r'/\*.*?\*/', '', col_expr, flags=re.DOTALL).strip()
                                    # Remove newlines and tabs - if there are newlines, it's probably not a column name
                                    if '\n' in col_expr or '\t' in col_expr or '\r' in col_expr:
                                        continue
                                    # Remove leading/trailing invalid characters
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove patterns like "abl.[column" -> "column"
                                    if '.' in col_expr and '[' in col_expr:
                                        parts = col_expr.split('.')
                                        col_expr = parts[-1].strip('[]').strip()
                                    # Validate: must be a valid column name (not SQL expression)
                                    # Must be a simple identifier (letters, numbers, underscore only)
                                    if col_expr and len(col_expr) >= 2 and len(col_expr) < 100 and \
                                       re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col_expr) and \
                                       not col_expr.upper().startswith('SELECT') and \
                                       not any(keyword in col_expr.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS', 'GROUP', 'BY', 'ORDER', 'HAVING']) and \
                                       col_expr not in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING'] and \
                                       not col_expr.startswith('(') and not col_expr.endswith(')') and \
                                       not col_expr.endswith('--') and not col_expr.startswith('['):
                                        col_names.append(col_expr)
                                # Register temp table with extracted columns
                                if col_names:
                                    self.temp_registry[temp_name] = col_names
                                    logger.debug(f"_parse_procedure_body_statements: Registered {temp_name} in temp_registry with {len(col_names)} columns from SQL fallback: {col_names[:5]}")
                                
                                # IMPORTANT: Extract dependencies REGARDLESS of whether col_names were found
                                # Dependencies are needed even if column extraction failed
                                # Extract dependencies from FROM and JOIN clauses (regex-based fallback for SELECT...INTO)
                                deps_from_sql: Set[str] = set()
                                try:
                                    sql_text = match.group(1)

                                    def _clean_dep(dep: str) -> str:
                                        dep = dep.strip().rstrip(';,')
                                        dep = dep.replace('[', '').replace(']', '')
                                        return dep
                                
                                    # Extract FROM clause table
                                    from_match = re.search(r'\bFROM\s+([\w.#@\[\]]+)', sql_text, re.IGNORECASE)
                                    if from_match:
                                        from_table = _clean_dep(from_match.group(1))
                                        if from_table and not from_table.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER']:
                                            deps_from_sql.add(from_table)
                                            logger.debug(f"_parse_procedure_body_statements: Found FROM table: {from_table} for {temp_name}")
                                
                                    # Extract all JOIN clause tables
                                    for join_match in re.finditer(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+([#\w.\[\]]+)(?:\s+(?:AS\s+)?[#\w.\[\]]+)?(?=\s+ON|\s+WHERE|\s+,|\s+JOIN|\s+;|\s*$)', sql_text, re.IGNORECASE):
                                        join_table = _clean_dep(join_match.group(1))
                                        if join_table and not join_table.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER']:
                                            deps_from_sql.add(join_table)
                                            logger.debug(f"_parse_procedure_body_statements: Found JOIN table: {join_table} for {temp_name}")

                                    # Broaden with basic dependency finder to catch bracketed identifiers and aliases
                                    deps_from_sql.update({
                                        _clean_dep(dep) for dep in self._extract_basic_dependencies(sql_text)
                                    })
                                
                                    # Store dependencies
                                    if not deps_from_sql:
                                        into_marker = f"INTO {temp_name}"
                                        pos = preprocessed_body.upper().find(into_marker.upper())
                                        if pos != -1:
                                            window = preprocessed_body[max(0, pos - 8000):pos + 8000]
                                            from_match = re.search(r'\bFROM\s+([\w.#@\[\]]+)', window, re.IGNORECASE)
                                            if from_match:
                                                deps_from_sql.add(_clean_dep(from_match.group(1)))
                                            for join_match in re.finditer(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+([#\w.\[\]]+)(?:\s+(?:AS\s+)?[#\w.\[\]]+)?(?=\s+ON|\s+WHERE|\s+,|\s+JOIN|\s+;|$)', window, re.IGNORECASE):
                                                jtbl = _clean_dep(join_match.group(1))
                                                if jtbl:
                                                    deps_from_sql.add(jtbl)
                                            deps_from_sql.update({
                                                _clean_dep(dep) for dep in self._extract_basic_dependencies(window)
                                            })
                                    if deps_from_sql:
                                        existing = set(self.temp_sources.get(temp_name, set()))
                                        existing.update(d for d in deps_from_sql if d)
                                        self.temp_sources[temp_name] = existing
                                        logger.debug(f"_parse_procedure_body_statements: Stored {len(deps_from_sql)} dependencies for {temp_name}")
                                    
                                    # CRITICAL: Generate ObjectInfo with facets for fallback path
                                    # Without this, temp tables won't appear in column graph
                                    # Check if we have columns OR dependencies (from temp_sources)
                                    has_deps = bool(self.temp_sources.get(temp_name))
                                    if col_names or has_deps:
                                        try:
                                            # Create schema facet
                                            schema_fields = []
                                            for col in col_names:
                                                schema_fields.append({
                                                    "name": col,
                                                    "type": "UNKNOWN",  # Type inference not available in fallback
                                                    "description": None
                                                })
                                            
                                            # Get qualified dependencies from temp_sources (includes all extracted deps)
                                            qualified_deps = set()
                                            all_deps = self.temp_sources.get(temp_name, set())
                                            for dep in all_deps:
                                                try:
                                                    qualified = self._get_table_name(dep)
                                                    # Use qualified name if available and not "unknown", otherwise use original dep
                                                    if qualified and qualified != "unknown":
                                                        qualified_deps.add(qualified)
                                                    else:
                                                        qualified_deps.add(dep)
                                                except Exception:
                                                    qualified_deps.add(dep)
                                            
                                            # Create canonical temp table name
                                            canonical_temp_name = self._canonical_temp_name(temp_name)
                                            ns_temp, name_temp = self._ns_and_name(canonical_temp_name, obj_type_hint="table")
                                            
                                            # Use dummy column if no columns extracted but we have dependencies
                                            if not col_names and qualified_deps:
                                                col_names = ["*"]
                                            
                                            # Create TableSchema
                                            columns_list = [ColumnSchema(name=col, data_type="UNKNOWN") for col in col_names]
                                            schema = TableSchema(
                                                namespace=ns_temp,
                                                name=name_temp,
                                                columns=columns_list
                                            )
                                            
                                            # Generate lineage for graph visualization
                                            lineage = []
                                            if qualified_deps and col_names:
                                                from infotracker.models import ColumnLineage, ColumnReference, TransformationType
                                                for col in col_names:
                                                    input_refs = []
                                                    for dep in qualified_deps:
                                                        try:
                                                            dep_ns, dep_name = self._ns_and_name(dep)
                                                            input_refs.append(ColumnReference(
                                                                namespace=dep_ns,
                                                                table_name=dep_name,
                                                                column_name="*"
                                                            ))
                                                        except Exception:
                                                            pass
                                                    if input_refs:
                                                        lineage.append(ColumnLineage(
                                                            output_column=col,
                                                            input_fields=input_refs,
                                                            transformation_type=TransformationType.UNKNOWN,
                                                            transformation_description="from temp source (fallback)"
                                                        ))
                                            
                                            # Create ObjectInfo with facets
                                            out_obj = ObjectInfo(
                                                name=name_temp,
                                                object_type="temp_table",
                                                schema=schema,
                                                lineage=lineage,
                                                dependencies=qualified_deps,
                                            )
                                            
                                            all_outputs.append(out_obj)
                                            all_inputs.update(qualified_deps)
                                            logger.debug(f"_parse_procedure_body_statements: Created ObjectInfo for {temp_name} from fallback with {len(col_names)} columns, {len(qualified_deps)} dependencies")
                                        except Exception as obj_error:
                                            logger.debug(f"_parse_procedure_body_statements: Failed to create ObjectInfo for {temp_name}: {obj_error}")
                                            import traceback
                                            logger.debug(f"_parse_procedure_body_statements: Traceback: {traceback.format_exc()}")
                                    
                                except Exception as deps_error:
                                    logger.debug(f"_parse_procedure_body_statements: Failed to extract dependencies for {temp_name}: {deps_error}")

                    except Exception as fallback_error:
                        logger.debug(f"_parse_procedure_body_statements: Failed to register temp table from SQL fallback: {fallback_error}")
        
        # First try splitting by semicolon
        chunks = preprocessed_body.split(';')
        
        # Additionally, split chunks that contain multiple statements based on keywords
        # Look for INSERT/UPDATE/DELETE/MERGE at line start
        expanded_chunks = []
        for chunk in chunks:
            # Find all DML statement starts
            dml_pattern = r'^\s*(INSERT\s+INTO|UPDATE\s+|DELETE\s+FROM|MERGE\s+)'
            matches = list(re.finditer(dml_pattern, chunk, re.MULTILINE | re.IGNORECASE))
            
            if len(matches) > 1:
                # Multiple DML statements in one chunk - split them
                prev_pos = 0
                for match in matches:
                    if match.start() > prev_pos:
                        expanded_chunks.append(chunk[prev_pos:match.start()])
                    prev_pos = match.start()
                expanded_chunks.append(chunk[prev_pos:])
            else:
                expanded_chunks.append(chunk)
        
        # Now parse each chunk
        seen_temp_tables_in_fallback = set()
        for chunk_idx, stmt_sql in enumerate(expanded_chunks):
            stmt_sql = stmt_sql.strip()
            if not stmt_sql or stmt_sql.upper() in ('GO', 'END'):
                continue
            # Check if this chunk contains SELECT INTO or UPDATE ... OUTPUT ... INTO
            if 'INTO #' in stmt_sql or 'INTO #asefl_temp' in stmt_sql:
                logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} contains 'INTO #' (length={len(stmt_sql)})")
            # Check if this chunk contains UPDATE ... OUTPUT ... INTO
            if 'UPDATE' in stmt_sql.upper() and 'OUTPUT' in stmt_sql.upper() and 'INTO #' in stmt_sql:
                logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} contains UPDATE ... OUTPUT ... INTO (length={len(stmt_sql)})")
                # Try to extract UPDATE ... OUTPUT ... INTO using string fallback
                try:
                    # Try to extract UPDATE ... FROM ... (may have OUTPUT ... INTO between SET and FROM)
                    u_lineage, u_cols, u_deps, u_target = self._extract_update_from_lineage_string(stmt_sql)
                    logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} UPDATE extraction: u_target={u_target}, u_lineage={len(u_lineage)} columns, u_deps={len(u_deps)} deps")
                    # If _extract_update_from_lineage_string failed, try to extract target table manually
                    if not u_target:
                        # Try to find UPDATE target table manually (before OUTPUT ... INTO)
                        update_match = re.search(r'(?is)\bUPDATE\s+([^\s\(,;]+)(?:\s+AS\s+(\w+)|\s+(\w+))?\s+SET\b', stmt_sql)
                        if update_match:
                            update_target_or_alias = self._normalize_table_ident(update_match.group(1))
                            update_alias = (update_match.group(2) or update_match.group(3) or '').strip() or update_target_or_alias
                            logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} UPDATE target/alias: {update_target_or_alias}, alias: {update_alias}")
                            # Extract dependencies and find actual table from FROM clause and JOINs
                            # Find FROM clause (may be before or after OUTPUT)
                            from_match = re.search(r'(?is)\bFROM\b(.*?)(?:\bOUTPUT\b|\bWHERE\b|$)', stmt_sql)
                            if from_match:
                                from_clause = from_match.group(1)
                                # Extract table names from FROM clause and JOINs, find table matching UPDATE alias
                                # Search in FROM and all JOIN types (INNER JOIN, LEFT JOIN, etc.)
                                for tbl_match in re.finditer(r'(?is)(?:FROM|(?:INNER|LEFT|RIGHT|FULL)?\s*JOIN)\s+([^\s,;()]+)(?:\s+AS\s+(\w+)|\s+(\w+))?', ' ' + from_clause):
                                    tbl = self._normalize_table_ident(tbl_match.group(1))
                                    tbl_alias = (tbl_match.group(2) or tbl_match.group(3) or '').strip() or tbl
                                    if tbl and not tbl.startswith('@') and '+' not in tbl:
                                        # Check if this table matches UPDATE alias
                                        if tbl_alias.lower() == update_alias.lower():
                                            # This is the UPDATE target table
                                            parts = tbl.split('.')
                                            if len(parts) >= 3:
                                                u_target = f"{parts[-2]}.{parts[-1]}"
                                            elif len(parts) == 2:
                                                u_target = tbl
                                            else:
                                                u_target = f"dbo.{tbl}"
                                            logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} Manually extracted UPDATE target: {u_target} (from alias {update_alias}, table={tbl})")
                                        # Add to dependencies (for temp tables, use canonical name)
                                        if tbl.startswith('#'):
                                            _, tbl = self._ns_and_name(tbl, obj_type_hint="temp_table")
                                        u_deps.add(tbl)
                            # If still no target found, use update_target_or_alias directly (might be table name, not alias)
                            if not u_target:
                                parts = update_target_or_alias.split('.')
                                if len(parts) >= 3:
                                    u_target = f"{parts[-2]}.{parts[-1]}"
                                elif len(parts) == 2:
                                    u_target = update_target_or_alias
                                else:
                                    u_target = f"dbo.{update_target_or_alias}"
                                logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} Manually extracted UPDATE target (direct): {u_target}")
                            logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} Extracted dependencies: {u_deps}")
                    # ALWAYS extract OUTPUT INTO when pattern detected (even if u_target is None)
                    # This ensures temp tables created via UPDATE...OUTPUT...INTO are registered in temp_lineage
                    if u_target or re.search(r'(?i)INTO\s+#', stmt_sql):
                        out_lineage, out_cols, out_deps, out_target = self._extract_output_into_lineage_string(stmt_sql)
                        logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} OUTPUT INTO extraction: out_target={out_target}, out_lineage={len(out_lineage)} columns, out_deps={len(out_deps)} deps")
                        if out_target:
                            # Create ObjectInfo for OUTPUT ... INTO target (temp table)
                            ns_out, nm_out = self._ns_and_name(out_target, obj_type_hint="temp_table")
                            out_schema = TableSchema(namespace=ns_out, name=nm_out, columns=out_cols or [])
                            out_obj = ObjectInfo(
                                name=nm_out,
                                object_type="temp_table",
                                schema=out_schema,
                                lineage=out_lineage or [],
                                dependencies=out_deps or set()
                            )
                            all_outputs.append(out_obj)
                            all_inputs.update(out_deps or set())
                            
                            # CRITICAL: Register in temp_lineage so subsequent INSERT SELECT can find lineage
                            # Extract simple temp name using _extract_temp_name to match lookup logic (e.g., #insert_update_temp_tetafk)
                            # For out_target="dbo.#insert_update_temp_tetafk", this extracts "insert_update_temp_tetafk" then prepends #
                            from .temp_utils import _extract_temp_name
                            temp_bare = self._extract_temp_name(out_target)
                            simple_key = f"#{temp_bare}" if not temp_bare.startswith('#') else temp_bare
                            if out_lineage:
                                # Build col_map from lineage (similar to _parse_select_into line 214)
                                col_map = {lin.output_column.lower() if lin.output_column else '': list(lin.input_fields or []) for lin in out_lineage}
                                self.temp_lineage[simple_key] = col_map
                                logger.debug(f"_parse_procedure_body_statements: Registered OUTPUT INTO temp_lineage for {simple_key}: {len(col_map)} columns")
                            
                            # Also register columns in temp_registry
                            if out_cols:
                                temp_cols = [col.name for col in out_cols]
                                self.temp_registry[simple_key] = temp_cols
                                logger.debug(f"_parse_procedure_body_statements: Registered OUTPUT INTO temp_registry for {simple_key}: {len(temp_cols)} columns")
                            
                            logger.debug(f"_parse_procedure_body_statements: Parsed UPDATE ... OUTPUT ... INTO from chunk {chunk_idx+1}, got obj: {out_obj.name}, dependencies={out_obj.dependencies}, lineage={len(out_obj.lineage)} columns")
                        # Also create ObjectInfo for UPDATE target (persistent table)
                        if u_target:
                            ns_tgt, nm_tgt = self._ns_and_name(u_target, obj_type_hint="table")
                            tgt_schema = TableSchema(namespace=ns_tgt, name=nm_tgt, columns=u_cols or [])
                            tgt_obj = ObjectInfo(
                                name=nm_tgt,
                                object_type="table",
                                schema=tgt_schema,
                                lineage=u_lineage or [],
                                dependencies=u_deps or set()
                            )
                            all_outputs.append(tgt_obj)
                            all_inputs.update(u_deps or set())
                            logger.debug(f"_parse_procedure_body_statements: Parsed UPDATE from chunk {chunk_idx+1}, got obj: {tgt_obj.name}, dependencies={tgt_obj.dependencies}, lineage={len(tgt_obj.lineage)} columns")
                except Exception as e:
                    logger.debug(f"_parse_procedure_body_statements: Failed to parse UPDATE ... OUTPUT ... INTO from chunk {chunk_idx+1}: {e}")
                    # Fallback: try to register temp table with columns extracted from OUTPUT ... INTO
                    try:
                        # Extract OUTPUT ... INTO #table pattern
                        output_into_match = re.search(r'(?is)OUTPUT\s+(.*?)\s+INTO\s+(#\w+)', stmt_sql, re.IGNORECASE)
                        if output_into_match:
                            temp_name = output_into_match.group(2)
                            output_cols_str = output_into_match.group(1)
                            # Extract column names from OUTPUT clause
                            col_names = []
                            for col_expr in re.split(r',\s*(?![^()]*\))', output_cols_str):
                                col_expr = col_expr.strip()
                                # Handle inserted.column or deleted.column
                                if '.' in col_expr:
                                    col_expr = col_expr.split('.')[-1]
                                # Remove brackets
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove SQL comments (-- and /* */)
                                    col_expr = re.sub(r'--.*$', '', col_expr, flags=re.MULTILINE).strip()
                                    col_expr = re.sub(r'/\*.*?\*/', '', col_expr, flags=re.DOTALL).strip()
                                    # Remove leading/trailing invalid characters
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove patterns like "abl.[column" -> "column"
                                    if '.' in col_expr and '[' in col_expr:
                                        parts = col_expr.split('.')
                                        col_expr = parts[-1].strip('[]').strip()
                                    # Validate: must be a valid column name (not SQL expression)
                                    if col_expr and len(col_expr) >= 2 and len(col_expr) < 100 and \
                                       not col_expr.upper().startswith('SELECT') and \
                                       not any(keyword in col_expr.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS']) and \
                                       col_expr not in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE'] and \
                                       not col_expr.startswith('(') and not col_expr.endswith(')') and \
                                       not col_expr.endswith('--') and not col_expr.startswith('['):
                                        col_names.append(col_expr)
                            # Register temp table with extracted columns
                            if col_names:
                                self.temp_registry[temp_name] = col_names
                                logger.debug(f"_parse_procedure_body_statements: Registered {temp_name} in temp_registry with {len(col_names)} columns from UPDATE ... OUTPUT ... INTO fallback: {col_names[:5]}")
                                # Also create ObjectInfo for this temp table so it can be found in all_outputs
                                try:
                                    ns_out, nm_out = self._ns_and_name(temp_name, obj_type_hint="temp_table")
                                    out_cols = [ColumnSchema(name=col, data_type=None, nullable=True, ordinal=i) for i, col in enumerate(col_names)]
                                    out_schema = TableSchema(namespace=ns_out, name=nm_out, columns=out_cols)
                                    out_obj = ObjectInfo(
                                        name=nm_out,
                                        object_type="temp_table",
                                        schema=out_schema,
                                        lineage=[],  # No lineage for OUTPUT INTO temp tables
                                        dependencies=set()  # Will be populated from UPDATE dependencies
                                    )
                                    all_outputs.append(out_obj)
                                    logger.debug(f"_parse_procedure_body_statements: Created ObjectInfo for {temp_name} from fallback: {out_obj.name}, columns={len(out_cols)}")
                                except Exception as obj_error:
                                    logger.debug(f"_parse_procedure_body_statements: Failed to create ObjectInfo for {temp_name} from fallback: {obj_error}")
                    except Exception as fallback_error:
                        logger.debug(f"_parse_procedure_body_statements: Failed to register temp table from UPDATE ... OUTPUT ... INTO fallback: {fallback_error}")
            try:
                # Force fallback for SELECT ... INTO to avoid sqlglot comment issues
                if re.search(r'(?is)SELECT\s+.*?\s+INTO\s+#', stmt_sql):
                    raise ValueError("Force fallback for SELECT INTO")

                parsed = sqlglot.parse_one(stmt_sql, read=self.dialect)
                if parsed:
                    statements.append(parsed)
                    if chunk_idx < 5:  # Log first 5 parsed statements
                        logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} parsed as {type(parsed).__name__}")
                else:
                    if 'INTO #' in stmt_sql:
                        logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} with 'INTO #' failed to parse")
            except Exception as e:
                # Skip unparseable statements
                if 'INTO #' in stmt_sql:
                    logger.debug(f"_parse_procedure_body_statements: Chunk {chunk_idx+1} with 'INTO #' raised exception: {e}")
                    # Fallback: try to register temp table with columns extracted from SQL
                    try:
                        # Try UPDATE ... OUTPUT ... INTO first
                        if 'UPDATE' in stmt_sql.upper() and 'OUTPUT' in stmt_sql.upper():
                            output_into_match = re.search(r'(?is)OUTPUT\s+(.*?)\s+INTO\s+(#\w+)', stmt_sql, re.IGNORECASE)
                            if output_into_match:
                                temp_name = output_into_match.group(2)
                                output_cols_str = output_into_match.group(1)
                                col_names = []
                                for col_expr in re.split(r',\s*(?![^()]*\))', output_cols_str):
                                    col_expr = col_expr.strip()
                                    if '.' in col_expr:
                                        col_expr = col_expr.split('.')[-1]
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove SQL comments (-- and /* */)
                                    col_expr = re.sub(r'--.*$', '', col_expr, flags=re.MULTILINE).strip()
                                    col_expr = re.sub(r'/\*.*?\*/', '', col_expr, flags=re.DOTALL).strip()
                                    # Remove leading/trailing invalid characters
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove patterns like "abl.[column" -> "column"
                                    if '.' in col_expr and '[' in col_expr:
                                        parts = col_expr.split('.')
                                        col_expr = parts[-1].strip('[]').strip()
                                    # Validate: must be a valid column name (not SQL expression)
                                    if col_expr and len(col_expr) >= 2 and len(col_expr) < 100 and \
                                       not col_expr.upper().startswith('SELECT') and \
                                       not any(keyword in col_expr.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS']) and \
                                       col_expr not in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE'] and \
                                       not col_expr.startswith('(') and not col_expr.endswith(')') and \
                                       not col_expr.endswith('--') and not col_expr.startswith('['):
                                        col_names.append(col_expr)
                                if col_names:
                                    self.temp_registry[temp_name] = col_names
                                    logger.debug(f"_parse_procedure_body_statements: Registered {temp_name} in temp_registry with {len(col_names)} columns from UPDATE ... OUTPUT ... INTO fallback (chunk exception): {col_names[:5]}")
                        # Try SELECT ... INTO
                        else:
                            select_into_match = re.search(r'(?is)SELECT\s+(.*?)\s+INTO\s+(#\w+)', stmt_sql, re.IGNORECASE)
                            if select_into_match:
                                temp_name = select_into_match.group(2)
                                
                                # If this is the first time we see this temp table in the fallback,
                                # clear any existing entry (which might be from a failed sqlglot prescan)
                                if temp_name not in seen_temp_tables_in_fallback:
                                    if temp_name in self.temp_sources:
                                        logger.debug(f"_parse_procedure_body_statements: Clearing potentially corrupted temp_sources for {temp_name} (fallback reset)")
                                        del self.temp_sources[temp_name]
                                    seen_temp_tables_in_fallback.add(temp_name)

                                col_list_str = select_into_match.group(1)
                                col_names = []
                                for col_expr in re.split(r',\s*(?![^()]*\))', col_list_str):
                                    # Remove SQL comments (-- and /* */) FIRST
                                    col_expr = re.sub(r'--.*$', '', col_expr, flags=re.MULTILINE)
                                    col_expr = re.sub(r'/\*.*?\*/', '', col_expr, flags=re.DOTALL)
                                    
                                    col_expr = col_expr.strip()
                                    # Skip if empty or too short
                                    if not col_expr or len(col_expr) < 2:
                                        continue
                                    # Remove newlines and tabs - if there are newlines, it's probably not a column name
                                    if '\n' in col_expr or '\t' in col_expr or '\r' in col_expr:
                                        continue
                                    # Remove AS alias if present
                                    if ' AS ' in col_expr.upper():
                                        col_expr = col_expr.rsplit(' AS ', 1)[-1].strip()
                                    elif ' ' in col_expr and not col_expr.startswith('['):
                                        # Might be alias without AS - take last word only if it's a simple identifier
                                        parts = col_expr.split()
                                        if len(parts) > 1:
                                            # Only use last part if it's a simple identifier (no special chars, no keywords)
                                            last_part = parts[-1].strip('[]').strip()
                                            if last_part and re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', last_part) and len(last_part) >= 2:
                                                col_expr = last_part
                                            else:
                                                # Skip this column - it's too complex
                                                continue
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove leading/trailing invalid characters
                                    col_expr = col_expr.strip('[]').strip()
                                    # Remove patterns like "abl.[column" -> "column"
                                    if '.' in col_expr and '[' in col_expr:
                                        parts = col_expr.split('.')
                                        col_expr = parts[-1].strip('[]').strip()
                                    # Validate: must be a valid column name (not SQL expression)
                                    # Must be a simple identifier (letters, numbers, underscore only)
                                    if col_expr and len(col_expr) >= 2 and len(col_expr) < 100 and \
                                       re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col_expr) and \
                                       not col_expr.upper().startswith('SELECT') and \
                                       not any(keyword in col_expr.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS', 'GROUP', 'BY', 'ORDER', 'HAVING']) and \
                                       col_expr not in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING'] and \
                                       not col_expr.startswith('(') and not col_expr.endswith(')') and \
                                       not col_expr.endswith('--') and not col_expr.startswith('['):
                                        col_names.append(col_expr)
                                
                                if col_names:
                                    self.temp_registry[temp_name] = col_names
                                    logger.debug(f"_parse_procedure_body_statements: Registered {temp_name} in temp_registry with {len(col_names)} columns from SELECT ... INTO fallback (chunk exception): {col_names[:5]}")
                                    
                                    # Extract dependencies from FROM and JOIN clauses (chunk exception fallback)
                                    deps_from_sql: Set[str] = set()
                                    try:
                                        # Strip comments from stmt_sql for dependency extraction to avoid false positives
                                        clean_stmt_sql = re.sub(r'--.*$', '', stmt_sql, flags=re.MULTILINE)
                                        clean_stmt_sql = re.sub(r'/\*.*?\*/', '', clean_stmt_sql, flags=re.DOTALL)
                                        
                                        def _clean_dep(dep: str) -> str:
                                            return dep.strip().rstrip(';,').replace('[', '').replace(']', '')
                                        
                                        # Extract FROM clause table
                                        from_match = re.search(r'\bFROM\s+([\w.#@\[\]]+)', clean_stmt_sql, re.IGNORECASE)
                                        if from_match:
                                            from_table = _clean_dep(from_match.group(1))
                                            if from_table and not from_table.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER']:
                                                deps_from_sql.add(from_table)
                                                logger.debug(f"_parse_procedure_body_statements: Found FROM table: {from_table} for {temp_name} (chunk exception fallback)")
                                        
                                        # Extract all JOIN clause tables
                                        for join_match in re.finditer(r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+([#\w.\[\]]+)(?:\s+(?:AS\s+)?[#\w.\[\]]+)?(?=\s+ON|\s+WHERE|\s+,|\s+JOIN|\s+;|\s*$)', clean_stmt_sql, re.IGNORECASE):
                                            join_table = _clean_dep(join_match.group(1))
                                            if join_table and not join_table.upper() in ['SELECT', 'WHERE', 'GROUP', 'ORDER']:
                                                deps_from_sql.add(join_table)
                                                logger.debug(f"_parse_procedure_body_statements: Found JOIN table: {join_table} for {temp_name} (chunk exception fallback)")
                                        
                                        if deps_from_sql:
                                            existing = set(self.temp_sources.get(temp_name, set()))
                                            existing.update(d for d in deps_from_sql if d)
                                            self.temp_sources[temp_name] = existing
                                            logger.debug(f"_parse_procedure_body_statements: Stored {len(deps_from_sql)} dependencies for {temp_name} from chunk exception fallback")
                                    except Exception as deps_error:
                                        logger.debug(f"_parse_procedure_body_statements: Failed to extract dependencies from chunk exception: {deps_error}")
                    except Exception as fallback_error:
                        logger.debug(f"_parse_procedure_body_statements: Failed to register temp table from chunk exception fallback: {fallback_error}")
                continue
    except Exception as e:
        logger.debug(f"_parse_procedure_body_statements: Exception during parsing: {e}")
        statements = []
    
    last_persistent_output = None
    best_match_output = None
    
    logger.debug(f"_parse_procedure_body_statements: Parsed {len(statements)} statements from body")
    
    for i, statement in enumerate(statements):
        stmt_type = type(statement).__name__
        if i < 10:  # Log first 10 statements
            logger.debug(f"_parse_procedure_body_statements: Statement {i+1}: {stmt_type}")
        
        # Handle WITH statement that contains SELECT INTO
        if isinstance(statement, exp.With):
            logger.debug(f"_parse_procedure_body_statements: Found WITH statement in body")
            # Check if the SELECT inside WITH has INTO
            if hasattr(statement, 'this') and isinstance(statement.this, exp.Select):
                logger.debug(f"_parse_procedure_body_statements: WITH statement has SELECT as this, checking for INTO...")
                if self._is_select_into(statement.this):
                    logger.debug(f"_parse_procedure_body_statements: Found SELECT INTO inside WITH in body")
                    obj = self._parse_select_into(statement.this, object_hint)
                    if obj:
                        all_outputs.append(obj)
                        all_inputs.update(obj.dependencies or [])
                else:
                    logger.debug(f"_parse_procedure_body_statements: SELECT inside WITH does not have INTO")
            # Check if WITH contains INSERT (WITH ... INSERT INTO ...)
            elif hasattr(statement, 'this') and isinstance(statement.this, exp.Insert):
                # Process CTEs from the WITH clause before parsing INSERT
                self._process_ctes(statement)
                logger.debug(f"_parse_procedure_body_statements: Processed CTEs from WITH ... INSERT, cte_registry keys: {list(self.cte_registry.keys())}")
                # Now parse the INSERT
                if self._is_insert_exec(statement.this):
                    obj = self._parse_insert_exec(statement.this, object_hint)
                else:
                    obj = self._parse_insert_select(statement.this, object_hint)
                if obj:
                    logger.debug(f"_parse_procedure_body_statements: Parsed INSERT INTO from WITH ... INSERT, got obj: {obj.name}, dependencies={obj.dependencies}, lineage={len(obj.lineage)} columns")
                    all_outputs.append(obj)
                    # Skip temp tables
                    if obj.name.startswith("#") or "tempdb" in obj.name.lower():
                        all_inputs.update(obj.dependencies or [])
                        continue
                    # Skip auxiliary tables
                    table_basename = obj.name.split('.')[-1].lower()
                    if any(suffix in table_basename for suffix in ['_ins_upd_results', '_results', '_log', '_audit']):
                        all_inputs.update(obj.dependencies or [])
                        continue
                    # This is a candidate persistent table
                    last_persistent_output = obj
                    proc_basename = object_hint.split('.')[-1].lower() if object_hint else ""
                    if proc_basename.startswith('update_') or proc_basename.startswith('load_'):
                        expected_table = proc_basename.replace('update_', '').replace('load_', '')
                        if table_basename == expected_table or table_basename.endswith(expected_table):
                            best_match_output = obj
                    all_inputs.update(obj.dependencies or [])
                continue
            else:
                # WITH statement without SELECT INTO or INSERT - process CTEs for later use
                self._process_ctes(statement)
                logger.debug(f"_parse_procedure_body_statements: Processed CTEs from standalone WITH, cte_registry keys: {list(self.cte_registry.keys())}")
        # Handle SELECT ... INTO #tmp
        elif isinstance(statement, exp.Select) and self._is_select_into(statement):
            logger.debug(f"_parse_procedure_body_statements: Found SELECT INTO in body")
            obj = self._parse_select_into(statement, object_hint)
            if obj:
                all_outputs.append(obj)
                all_inputs.update(obj.dependencies or [])
        # Handle UPDATE ... OUTPUT ... INTO
        elif isinstance(statement, exp.Update):
            logger.debug(f"_parse_procedure_body_statements: Found UPDATE statement (AST-based)")
            # Check if UPDATE has OUTPUT ... INTO clause
            # In T-SQL, OUTPUT ... INTO is not always in 'returning' attribute, so check SQL string too
            update_sql = str(statement)
            has_output_into = (hasattr(statement, 'returning') and statement.returning) or ('OUTPUT' in update_sql.upper() and 'INTO' in update_sql.upper())
            logger.debug(f"_parse_procedure_body_statements: UPDATE has_output_into={has_output_into}, returning={hasattr(statement, 'returning') and statement.returning}, SQL contains OUTPUT/INTO={'OUTPUT' in update_sql.upper() and 'INTO' in update_sql.upper()}")
            if has_output_into:
                # Try to extract UPDATE ... OUTPUT ... INTO lineage
                try:
                    update_sql = str(statement)
                    u_lineage, u_cols, u_deps, u_target = self._extract_update_from_lineage_string(update_sql)
                    if u_target:
                        # Also check for OUTPUT ... INTO
                        out_lineage, out_cols, out_deps, out_target = self._extract_output_into_lineage_string(update_sql)
                        if out_target:
                            # Create ObjectInfo for OUTPUT ... INTO target (temp table)
                            ns_out, nm_out = self._ns_and_name(out_target, obj_type_hint="temp_table")
                            out_schema = TableSchema(namespace=ns_out, name=nm_out, columns=out_cols or [])
                            out_obj = ObjectInfo(
                                name=nm_out,
                                object_type="temp_table",
                                schema=out_schema,
                                lineage=out_lineage or [],
                                dependencies=out_deps or set()
                            )
                            all_outputs.append(out_obj)
                            all_inputs.update(out_deps or set())
                            logger.debug(f"_parse_procedure_body_statements: Parsed UPDATE ... OUTPUT ... INTO, got obj: {out_obj.name}, dependencies={out_obj.dependencies}, lineage={len(out_obj.lineage)} columns")
                    # Also create ObjectInfo for UPDATE target (persistent table)
                    if u_target:
                        ns_tgt, nm_tgt = self._ns_and_name(u_target, obj_type_hint="table")
                        tgt_schema = TableSchema(namespace=ns_tgt, name=nm_tgt, columns=u_cols or [])
                        tgt_obj = ObjectInfo(
                            name=nm_tgt,
                            object_type="table",
                            schema=tgt_schema,
                            lineage=u_lineage or [],
                            dependencies=u_deps or set()
                        )
                        all_outputs.append(tgt_obj)
                        all_inputs.update(u_deps or set())
                        # Check if this is a candidate persistent table
                        table_basename = nm_tgt.split('.')[-1].lower()
                        if not any(suffix in table_basename for suffix in ['_ins_upd_results', '_results', '_log', '_audit']):
                            last_persistent_output = tgt_obj
                            proc_basename = object_hint.split('.')[-1].lower() if object_hint else ""
                            if proc_basename.startswith('update_') or proc_basename.startswith('load_'):
                                expected_table = proc_basename.replace('update_', '').replace('load_', '')
                                if table_basename == expected_table or table_basename.endswith(expected_table):
                                    best_match_output = tgt_obj
                        logger.debug(f"_parse_procedure_body_statements: Parsed UPDATE, got obj: {tgt_obj.name}, dependencies={tgt_obj.dependencies}, lineage={len(tgt_obj.lineage)} columns")
                except Exception as e:
                    logger.debug(f"_parse_procedure_body_statements: Failed to parse UPDATE: {e}")
                    import traceback
                    logger.debug(f"_parse_procedure_body_statements: Traceback: {traceback.format_exc()}")
        # Handle INSERT INTO
        elif isinstance(statement, exp.Insert):
            logger.debug(f"_parse_procedure_body_statements: Found INSERT statement, target={self._get_table_name(statement.this, object_hint) if hasattr(statement, 'this') else 'unknown'}")
            # Check if INSERT has a parent WITH clause (CTE defined before INSERT)
            parent = getattr(statement, 'parent', None)
            if isinstance(parent, exp.With):
                # Process CTEs from the parent WITH clause before parsing INSERT
                self._process_ctes(parent)
                logger.debug(f"_parse_procedure_body_statements: Processed CTEs from parent WITH for INSERT, cte_registry keys: {list(self.cte_registry.keys())}")
            # Also check if previous statement was a WITH (CTE defined before INSERT in separate statement)
            elif i > 0 and isinstance(statements[i-1], exp.With):
                # Process CTEs from the previous WITH statement
                self._process_ctes(statements[i-1])
                logger.debug(f"_parse_procedure_body_statements: Processed CTEs from previous WITH statement for INSERT, cte_registry keys: {list(self.cte_registry.keys())}")
            # Also check if INSERT is inside a WITH statement (WITH ... INSERT INTO ...)
            # This happens when SQLGlot parses "WITH ... INSERT INTO ..." as exp.With containing exp.Insert
            # but the INSERT is not a direct child - we need to walk up the tree
            else:
                # Walk up the tree to find a WITH parent
                current = statement
                while current:
                    current_parent = getattr(current, 'parent', None)
                    if isinstance(current_parent, exp.With):
                        # Process CTEs from the WITH clause
                        self._process_ctes(current_parent)
                        logger.debug(f"_parse_procedure_body_statements: Processed CTEs from WITH parent (walked up tree) for INSERT, cte_registry keys: {list(self.cte_registry.keys())}")
                        break
                    current = current_parent
                # Fallback: if CTE registry is still empty, try to extract CTE from SQL string
                if not self.cte_registry:
                    logger.debug(f"_parse_procedure_body_statements: CTE registry is empty, trying SQL string fallback for INSERT")
                    try:
                        # Get the SQL string for this INSERT statement
                        insert_sql = str(statement)
                        logger.debug(f"_parse_procedure_body_statements: INSERT SQL (first 200 chars): {insert_sql[:200]}")
                        # Check if INSERT SQL itself contains WITH clause
                        if 'WITH' in insert_sql.upper() and 'AS' in insert_sql.upper():
                            logger.debug(f"_parse_procedure_body_statements: INSERT SQL contains WITH clause, trying to parse it")
                            # Try to parse the WITH clause from INSERT SQL
                            try:
                                # Find "WITH name AS (" pattern
                                with_start_match = re.search(r'(?is)WITH\s+(\w+)\s+AS\s*\(', insert_sql)
                                if with_start_match:
                                    cte_name = with_start_match.group(1)
                                    with_start_pos = with_start_match.start()
                                    logger.debug(f"_parse_procedure_body_statements: Found WITH {cte_name} in INSERT SQL at position {with_start_pos}")
                                    # Find the matching closing parenthesis
                                    paren_count = 0
                                    paren_pos = with_start_match.end() - 1  # Position of opening (
                                    with_end_pos = -1
                                    for i in range(paren_pos, len(insert_sql)):
                                        if insert_sql[i] == '(':
                                            paren_count += 1
                                        elif insert_sql[i] == ')':
                                            paren_count -= 1
                                            if paren_count == 0:
                                                # Found matching closing parenthesis
                                                with_end_pos = i + 1
                                                break
                                    
                                    if with_end_pos > 0:
                                        with_stmt_str = insert_sql[with_start_pos:with_end_pos]
                                        logger.debug(f"_parse_procedure_body_statements: Extracted WITH statement (length={len(with_stmt_str)}, first 200 chars: {with_stmt_str[:200]})")
                                        # Try to parse the WITH statement
                                        try:
                                            # Add INSERT to make it a complete statement
                                            complete_with = f"{with_stmt_str} INSERT INTO dummy SELECT * FROM {cte_name}"
                                            parsed_stmt = sqlglot.parse_one(complete_with, read=self.dialect)
                                            # SQLGlot may parse "WITH ... INSERT INTO ..." as exp.Insert with WITH clause
                                            if isinstance(parsed_stmt, exp.With):
                                                self._process_ctes(parsed_stmt)
                                                logger.debug(f"_parse_procedure_body_statements: Processed CTEs from INSERT SQL WITH clause (exp.With), cte_registry keys: {list(self.cte_registry.keys())}")
                                            elif isinstance(parsed_stmt, exp.Insert):
                                                # Check if INSERT has a WITH clause
                                                if hasattr(parsed_stmt, 'with') and getattr(parsed_stmt, 'with', None):
                                                    self._process_ctes(parsed_stmt)
                                                    logger.debug(f"_parse_procedure_body_statements: Processed CTEs from INSERT SQL WITH clause (exp.Insert.with), cte_registry keys: {list(self.cte_registry.keys())}")
                                                else:
                                                    # Try to extract WITH from the original statement
                                                    logger.debug(f"_parse_procedure_body_statements: Parsed statement is exp.Insert but has no WITH clause, trying to extract FROM clause from CTE definition")
                                                    # Extract FROM clause from CTE definition
                                                    # Look for FROM followed by table name (may be temp table with or without #, with or without brackets)
                                                    # Pattern: FROM [whitespace] [optional bracket] [optional #] table_name [optional bracket]
                                                    from_match = re.search(rf'(?is)FROM\s+(\[?#?[\w]+\]?)', with_stmt_str, re.IGNORECASE)
                                                    if from_match:
                                                        from_table = from_match.group(1).strip().strip('[]')
                                                        # Check if it's a temp table (starts with # or is in temp_registry)
                                                        temp_table_name = from_table if from_table.startswith('#') else f"#{from_table}"
                                                        if from_table.startswith('#') or (temp_table_name in self.temp_registry):
                                                            logger.debug(f"_parse_procedure_body_statements: CTE {cte_name} sources temp table {temp_table_name} (from INSERT SQL string, with_stmt_str length={len(with_stmt_str)})")
                                                            # Create a simple SELECT statement that references the temp table
                                                            try:
                                                                temp_select = sqlglot.parse_one(f"SELECT * FROM {temp_table_name}", read=self.dialect)
                                                                if isinstance(temp_select, exp.Select):
                                                                    # Store the CTE definition in registry
                                                                    self.cte_registry[cte_name] = {
                                                                        'columns': [],
                                                                        'definition': temp_select
                                                                    }
                                                                    logger.debug(f"_parse_procedure_body_statements: Registered CTE {cte_name} with temp table {temp_table_name} dependency, cte_registry keys: {list(self.cte_registry.keys())}")
                                                            except Exception as reg_error:
                                                                logger.debug(f"_parse_procedure_body_statements: Failed to register CTE: {reg_error}")
                                                        else:
                                                            logger.debug(f"_parse_procedure_body_statements: FROM table {from_table} is not a temp table (not in temp_registry: {list(self.temp_registry.keys())[:5]})")
                                                    else:
                                                        logger.debug(f"_parse_procedure_body_statements: No FROM clause found in CTE definition for {cte_name}, with_stmt_str (last 200 chars): {with_stmt_str[-200:]}")
                                            else:
                                                logger.debug(f"_parse_procedure_body_statements: Parsed statement is not exp.With or exp.Insert, type: {type(parsed_stmt).__name__}")
                                        except Exception as parse_error:
                                            logger.debug(f"_parse_procedure_body_statements: Failed to parse WITH from INSERT SQL: {parse_error}")
                                            # If parsing fails, try to extract CTE dependencies from the SQL string
                                            # Look for FROM clause in the CTE definition
                                            from_match = re.search(rf'(?is)FROM\s+([^\s\)]+)', with_stmt_str, re.IGNORECASE)
                                            if from_match:
                                                from_table = from_match.group(1).strip().strip('[]')
                                                # Check if it's a temp table
                                                if from_table.startswith('#'):
                                                    logger.debug(f"_parse_procedure_body_statements: CTE {cte_name} sources temp table {from_table} (from INSERT SQL string)")
                                                    # Try to create a minimal CTE definition for registry
                                                    # We'll store just the temp table dependency
                                                    try:
                                                        # Create a simple SELECT statement that references the temp table
                                                        temp_select = sqlglot.parse_one(f"SELECT * FROM {from_table}", read=self.dialect)
                                                        if isinstance(temp_select, exp.Select):
                                                            # Store the CTE definition in registry
                                                            self.cte_registry[cte_name] = {
                                                                'columns': [],
                                                                'definition': temp_select
                                                            }
                                                            logger.debug(f"_parse_procedure_body_statements: Registered CTE {cte_name} with temp table {from_table} dependency")
                                                    except Exception as reg_error:
                                                        logger.debug(f"_parse_procedure_body_statements: Failed to register CTE: {reg_error}")
                                    else:
                                        logger.debug(f"_parse_procedure_body_statements: Could not find matching closing parenthesis for WITH {cte_name} in INSERT SQL")
                            except Exception as e:
                                logger.debug(f"_parse_procedure_body_statements: Exception extracting WITH from INSERT SQL: {e}")
                        
                        # Also try to find WITH in preprocessed_body before INSERT
                        insert_target = self._get_table_name(statement.this, object_hint) if hasattr(statement, 'this') else 'unknown'
                        # Try different search patterns
                        search_patterns = [
                            f"INSERT INTO {insert_target}",
                            f"INSERT INTO [dbo].[{insert_target.split('.')[-1]}]",
                            f"INSERT INTO dbo.{insert_target.split('.')[-1]}",
                            insert_target.split('.')[-1] if '.' in insert_target else insert_target
                        ]
                        insert_pos = -1
                        for pattern in search_patterns:
                            insert_pos = preprocessed_body.find(pattern)
                            if insert_pos > 0:
                                logger.debug(f"_parse_procedure_body_statements: Found '{pattern}' at position: {insert_pos}")
                                break
                        
                        if insert_pos > 0:
                            # Look backwards for WITH clause
                            before_insert = preprocessed_body[:insert_pos]
                            logger.debug(f"_parse_procedure_body_statements: Before INSERT (last 500 chars): {before_insert[-500:]}")
                            # Find the last WITH statement before this INSERT
                            # Use a pattern that finds WITH ... AS (...) followed by INSERT
                            # We need to handle nested parentheses, so we'll use a simpler approach:
                            # Find "WITH name AS" and then find the matching closing parenthesis
                            with_pattern = r'(?is);?\s*WITH\s+(\w+)\s+AS\s*\('
                            with_start_match = re.search(with_pattern, before_insert)
                            if with_start_match:
                                cte_name = with_start_match.group(1)
                                with_start_pos = with_start_match.start()
                                logger.debug(f"_parse_procedure_body_statements: Found WITH {cte_name} at position {with_start_pos}")
                                # Find the matching closing parenthesis
                                # Count opening and closing parentheses
                                paren_count = 0
                                paren_pos = with_start_match.end() - 1  # Position of opening (
                                for i in range(paren_pos, len(before_insert)):
                                    if before_insert[i] == '(':
                                        paren_count += 1
                                    elif before_insert[i] == ')':
                                        paren_count -= 1
                                        if paren_count == 0:
                                            # Found matching closing parenthesis
                                            with_end_pos = i + 1
                                            with_stmt_str = before_insert[with_start_pos:with_end_pos]
                                            logger.debug(f"_parse_procedure_body_statements: Extracted WITH statement (length={len(with_stmt_str)}, first 200 chars: {with_stmt_str[:200]})")
                                            with_match = True
                                            break
                                else:
                                    # Didn't find matching closing parenthesis in before_insert, try to find it after INSERT
                                    logger.debug(f"_parse_procedure_body_statements: Could not find matching closing parenthesis in before_insert, trying after INSERT")
                                    with_match = False
                            else:
                                logger.debug(f"_parse_procedure_body_statements: No WITH pattern found before INSERT")
                                with_match = False
                            
                            if with_match and 'with_stmt_str' in locals():
                                logger.debug(f"_parse_procedure_body_statements: Found WITH {cte_name} before INSERT in SQL string, trying to parse it")
                                # Try to parse the WITH statement
                                try:
                                    with_stmt = sqlglot.parse_one(with_stmt_str, read=self.dialect)
                                    if isinstance(with_stmt, exp.With):
                                        self._process_ctes(with_stmt)
                                        logger.debug(f"_parse_procedure_body_statements: Processed CTEs from SQL string fallback, cte_registry keys: {list(self.cte_registry.keys())}")
                                except Exception as parse_error:
                                    logger.debug(f"_parse_procedure_body_statements: Failed to parse WITH from SQL string: {parse_error}")
                                    # If parsing fails, try to extract CTE dependencies from the SQL string
                                    # Look for FROM clause in the CTE definition
                                    from_match = re.search(rf'(?is)WITH\s+{re.escape(cte_name)}\s+AS\s*\([^)]*FROM\s+([^\s\)]+)', with_stmt_str, re.IGNORECASE)
                                    if from_match:
                                        from_table = from_match.group(1).strip().strip('[]')
                                        # Check if it's a temp table
                                        if from_table.startswith('#'):
                                            # Register the CTE with the temp table as source
                                            logger.debug(f"_parse_procedure_body_statements: CTE {cte_name} sources temp table {from_table} (from SQL string)")
                                            # We can't fully process the CTE without AST, but we can at least note the dependency
                    except Exception as e:
                        logger.debug(f"_parse_procedure_body_statements: Exception in CTE fallback: {e}")
                        import traceback
                        logger.debug(f"_parse_procedure_body_statements: CTE fallback traceback: {traceback.format_exc()}")
                else:
                    logger.debug(f"_parse_procedure_body_statements: CTE registry is not empty, skipping fallback. Keys: {list(self.cte_registry.keys())}")
            if self._is_insert_exec(statement):
                obj = self._parse_insert_exec(statement, object_hint)
            else:
                # Log details before parsing INSERT SELECT
                raw_target = self._get_table_name(statement.this, object_hint) if hasattr(statement, 'this') else None
                has_expression = hasattr(statement, 'expression') and statement.expression is not None
                expr_type = type(statement.expression).__name__ if has_expression else 'None'
                logger.debug(f"_parse_procedure_body_statements: About to parse INSERT SELECT, raw_target={raw_target}, has_expression={has_expression}, expr_type={expr_type}")
                obj = self._parse_insert_select(statement, object_hint)
            
            if obj:
                logger.debug(f"_parse_procedure_body_statements: Parsed INSERT INTO, got obj: {obj.name}, dependencies={obj.dependencies}, lineage={len(obj.lineage)} columns")
                all_outputs.append(obj)
                # Skip temp tables and table variables
                if obj.name.startswith("#") or obj.name.startswith("@") or "tempdb" in obj.name.lower():
                    all_inputs.update(obj.dependencies or [])
                    continue
                
                # All persistent tables are valid outputs (including _ins_upd_results, _results, etc.)
                # This is a candidate persistent table
                last_persistent_output = obj
                
                # If table name matches procedure name pattern (e.g., update_X_BV -> X_BV),
                # it's likely the main target
                table_basename = obj.name.split('.')[-1].lower()
                proc_basename = object_hint.split('.')[-1].lower() if object_hint else ""
                if proc_basename.startswith('update_') or proc_basename.startswith('load_'):
                    expected_table = proc_basename.replace('update_', '').replace('load_', '')
                    if table_basename == expected_table or table_basename.endswith(expected_table):
                        best_match_output = obj
                
                all_inputs.update(obj.dependencies or [])
    
    # Create ObjectInfo for temp tables from temp_registry that are not in all_outputs
    # This ensures temp tables like #insert_update_temp_asefl, #MaxLoadDate, #MinAccountingPeriod are available
    # Skip table variables (starting with @) - they should not be materialized
    for tkey in self.temp_registry.keys():
        if tkey.startswith('#') and not tkey.startswith('@'):
            # Check if this temp table is already in all_outputs
            temp_already_in_outputs = False
            if tkey == "#offer":  # Debug #offer specifically
                logger.debug(f"_parse_procedure_body_statements: Checking {tkey}, all_outputs count={len(all_outputs)}")
                # Log first 3 temp_table objects to understand structure
                temp_count = 0
                for obj in all_outputs:
                    if obj and obj.object_type == "temp_table":
                        temp_count += 1
                        if temp_count <= 3:
                            has_schema = obj.schema is not None
                            has_schema_name = (obj.schema.name if has_schema else None)
                            logger.debug(f"  Temp #{temp_count}: name={obj.name}, has_schema={has_schema}, schema.name={has_schema_name}")
            for obj in all_outputs:
                # Ensure we have a valid object and schema
                if not (obj and obj.object_type == "temp_table"):
                    continue
                
                # Get schema name safely
                schema_name = obj.schema.name if (obj.schema and obj.schema.name) else obj.name
                if not schema_name:
                    continue
                    
                if tkey == "#offer":  # Debug only for #offer
                    logger.debug(f"_parse_procedure_body_statements: Checking {tkey} against schema_name={schema_name}, obj.name={obj.name}")
                    logger.debug(f"  Comparison: schema_lower={schema_name_lower}, tkey_lower={tkey_lower}")
                    logger.debug(f"  endswith(tkey_lower)={schema_name_lower.endswith(tkey_lower)}")
                    logger.debug(f"  endswith(.tkey_no_hash)={schema_name_lower.endswith(f'.{tkey_no_hash_lower}')}")

                # Case-insensitive comparison
                schema_name_lower = schema_name.lower()
                tkey_lower = tkey.lower()
                tkey_no_hash_lower = tkey.lstrip('#').lower()
                
                # Check for exact match or suffix match
                # e.g. "dbo.proc#offer" ends with "#offer"
                if (schema_name_lower == tkey_lower or
                    schema_name_lower.endswith(tkey_lower) or 
                    schema_name_lower.endswith(f".{tkey_no_hash_lower}") or
                    schema_name_lower == tkey_no_hash_lower or
                    obj.name.lower() == tkey_lower):
                    
                    temp_already_in_outputs = True
                    logger.debug(f"_parse_procedure_body_statements: Temp table {tkey} already in all_outputs as {schema_name}")
                    break
            if not temp_already_in_outputs:
                try:
                    ns_out, nm_out = self._ns_and_name(tkey, obj_type_hint="temp_table")
                    col_names = self.temp_registry[tkey]
                    
                    # Use dummy column if no columns extracted
                    if not col_names:
                        col_names = ["*"]
                        # Update registry so engine.py sees it too
                        self.temp_registry[tkey] = col_names
                        
                    out_cols = [ColumnSchema(name=col, data_type=None, nullable=True, ordinal=i) for i, col in enumerate(col_names)]

                    out_schema = TableSchema(namespace=ns_out, name=nm_out, columns=out_cols)
                    out_obj = ObjectInfo(
                        name=nm_out,
                        object_type="temp_table",
                        schema=out_schema,
                        lineage=[],  # No lineage for temp tables from temp_registry
                        dependencies=set()  # Will be populated from temp_sources if available
                    )
                    # Try to get dependencies from temp_sources, otherwise derive from the nearest SELECT/INSERT segment
                    deps_for_temp: Set[str] = set()
                    if tkey in self.temp_sources:
                        deps_for_temp = set(self.temp_sources[tkey])
                    else:
                        try:
                            import re as _re
                            search_text = full_sql or body_sql
                            if search_text:
                                pattern_sel = rf"(?is)\\bSELECT\\s+.*?\\bINTO\\s+#{_re.escape(tkey.lstrip('#'))}\\b.*?(?=;|\\b(?:INSERT|CREATE|ALTER|UPDATE|DELETE|DROP|TRUNCATE|BEGIN|END|GO|COMMIT|ROLLBACK)\\b|$)"
                                for m_tmp in _re.finditer(pattern_sel, search_text):
                                    seg_sql = m_tmp.group(0)
                                    deps_for_temp.update(self._extract_basic_dependencies(seg_sql))
                                    if deps_for_temp:
                                        break
                                if not deps_for_temp:
                                    pattern_ins = rf"(?is)\\bINSERT\\s+INTO\\s+#{_re.escape(tkey.lstrip('#'))}\\b.*?(?=;|\\bINSERT\\b|\\bCREATE\\b|\\bALTER\\b|\\bUPDATE\\b|\\bDELETE\\b|\\bEND\\b|\\bGO\\b|$)"
                                    for m_tmp in _re.finditer(pattern_ins, search_text):
                                        seg_sql = m_tmp.group(0)
                                        deps_for_temp.update(self._extract_basic_dependencies(seg_sql))
                                        if deps_for_temp:
                                            break
                        except Exception:
                            pass
                        if deps_for_temp:
                            deps_for_temp = {d for d in deps_for_temp if '#' not in d and 'tempdb' not in str(d).lower()}
                            self.temp_sources[tkey] = deps_for_temp
                    if deps_for_temp:
                        out_obj.dependencies = deps_for_temp
                        
                        # Generate lineage for graph visualization
                        if out_cols:
                            from infotracker.models import ColumnLineage, ColumnReference, TransformationType
                            lineage = []
                            for col in out_cols:
                                input_refs = []
                                for dep in deps_for_temp:
                                    try:
                                        dep_ns, dep_name = self._ns_and_name(dep)
                                        input_refs.append(ColumnReference(
                                            namespace=dep_ns,
                                            table_name=dep_name,
                                            column_name="*"
                                        ))
                                    except Exception:
                                        pass
                                if input_refs:
                                    lineage.append(ColumnLineage(
                                        output_column=col.name,
                                        input_fields=input_refs,
                                        transformation_type=TransformationType.UNKNOWN,
                                        transformation_description="from temp source"
                                    ))
                            out_obj.lineage = lineage
                            
                            # Register in temp_lineage so downstream usage can resolve it
                            # This is crucial for graph connectivity when this temp table is used as a source
                            col_map = {}
                            for lin in lineage:
                                col_map[lin.output_column] = lin.input_fields
                            
                            self.temp_lineage[tkey] = col_map
                            # Also register canonical name
                            try:
                                canonical = self._canonical_temp_name(tkey)
                                self.temp_lineage[canonical] = col_map
                            except Exception:
                                pass
                    all_outputs.append(out_obj)
                    logger.debug(f"_parse_procedure_body_statements: Created ObjectInfo for {tkey} from temp_registry: {out_obj.name}, columns={len(out_cols)}, dependencies={len(out_obj.dependencies)}")
                except Exception as obj_error:
                    logger.debug(f"_parse_procedure_body_statements: Failed to create ObjectInfo for {tkey} from temp_registry: {obj_error}")
    
    # Prefer best match (procedure name → table name), otherwise use last persistent
    # HEURISTIC: If we have multiple persistent outputs, prioritize the one that:
    # 1. Has lineage (columns extracted)
    # 2. Matches the procedure name pattern
    # 3. Is the first one found (often the main INSERT)
    
    # Filter for persistent tables
    persistent_outputs = [o for o in all_outputs if o.object_type == "table"]
    
    # 1. Filter for outputs with lineage
    outputs_with_lineage = [o for o in persistent_outputs if o.lineage and len(o.lineage) > 0]
    
    # 2. Filter for name match
    proc_basename = object_hint.split('.')[-1].lower() if object_hint else ""
    expected_table = proc_basename.replace('update_', '').replace('load_', '')
    
    matches_name = []
    candidates = outputs_with_lineage if outputs_with_lineage else persistent_outputs
    
    for o in candidates:
        table_basename = o.name.split('.')[-1].lower()
        # Check if table name is contained in procedure name or vice versa (ignoring update/load prefix)
        # Also check for fuzzy match (e.g. asefl_TrialBalance_BV vs TrialBalance_asefl_BV)
        if (expected_table and (expected_table == table_basename or 
            table_basename.endswith(expected_table) or 
            expected_table in table_basename or
            table_basename in expected_table)):
            matches_name.append(o)
            
    # Selection logic:
    if matches_name:
        # If we have name matches with lineage (or without if none have lineage), pick the first one
        # But prefer the one that is NOT _ins_upd_results if possible, unless it's the only one
        non_aux_matches = [o for o in matches_name if "_ins_upd_results" not in o.name.lower()]
        if non_aux_matches:
            result_output = non_aux_matches[0]
        else:
            result_output = matches_name[0]
    elif outputs_with_lineage:
        # If no name match, but some have lineage, pick the first one with lineage
        result_output = outputs_with_lineage[0]
    else:
        # Fallback to original logic
        result_output = best_match_output or last_persistent_output
        
    logger.debug(f"Selected primary output: {result_output.name if result_output else 'None'} (lineage count: {len(result_output.lineage) if result_output else 0})")
    
    # If we found persistent outputs, merge lineage from UPDATE if it targets the same table
    if result_output:
        # Check if there's an UPDATE that targets the same table
        result_table_name = result_output.schema.name if result_output.schema else result_output.name
        update_obj = None
        insert_update_temp_obj = None
        logger.debug(f"_parse_procedure_body_statements: Looking for UPDATE and #insert_update_temp_asefl for {result_table_name}, all_outputs count: {len(all_outputs)}")
        for obj in all_outputs:
            if obj and obj.schema:
                logger.debug(f"_parse_procedure_body_statements: Checking obj: {obj.object_type}, name={obj.schema.name}")
            if (obj and obj.object_type == "table" and obj.schema and 
                obj.schema.name == result_table_name and obj != result_output):
                # This is an UPDATE targeting the same table
                update_obj = obj
                logger.debug(f"_parse_procedure_body_statements: Found update_obj: {update_obj.name}, lineage={len(update_obj.lineage)} columns")
            elif (obj and obj.object_type == "temp_table" and obj.schema and
                  'insert_update_temp_asefl' in obj.schema.name.lower()):
                # Found #insert_update_temp_asefl
                insert_update_temp_obj = obj
                logger.debug(f"_parse_procedure_body_statements: Found insert_update_temp_obj: {insert_update_temp_obj.name}, dependencies={insert_update_temp_obj.dependencies}")
        
        # If UPDATE exists and #insert_update_temp_asefl exists, merge lineage
        # UPDATE modifies TrialBalance_asefl_BV and OUTPUT INTO #insert_update_temp_asefl
        # So columns updated by UPDATE should have lineage through #insert_update_temp_asefl
        if update_obj and insert_update_temp_obj and update_obj.lineage:
            if not result_output.lineage:
                result_output.lineage = []
            # For columns updated by UPDATE, lineage should go through #insert_update_temp_asefl
            for lin in update_obj.lineage:
                # Replace input_fields to point to #insert_update_temp_asefl instead of direct sources
                from ..models import ColumnLineage, ColumnReference, TransformationType
                updated_input_fields = []
                # If lineage has input_fields pointing to #asefl_temp or other sources,
                # replace them with #insert_update_temp_asefl
                if lin.input_fields:
                    for input_field in lin.input_fields:
                        # Replace with #insert_update_temp_asefl reference
                        updated_input_fields.append(ColumnReference(
                            namespace=insert_update_temp_obj.schema.namespace,
                            table_name=insert_update_temp_obj.schema.name,
                            column_name=input_field.column_name
                        ))
                else:
                    # No input_fields, use column name from output
                    updated_input_fields.append(ColumnReference(
                        namespace=insert_update_temp_obj.schema.namespace,
                        table_name=insert_update_temp_obj.schema.name,
                        column_name=lin.output_column
                    ))
                
                # Check if this column already exists in result_output.lineage
                existing = next((l for l in result_output.lineage if l.output_column == lin.output_column), None)
                if existing:
                    # Merge input_fields - add #insert_update_temp_asefl as source
                    if not existing.input_fields:
                        existing.input_fields = []
                    existing.input_fields.extend(updated_input_fields)
                else:
                    # Add new lineage entry
                    result_output.lineage.append(ColumnLineage(
                        output_column=lin.output_column,
                        input_fields=updated_input_fields,
                        transformation_type=lin.transformation_type,
                        transformation_description=f"from UPDATE through {insert_update_temp_obj.schema.name}"
                    ))
        # Expand temp dependencies on the final output, if any
        # Keep temp tables in dependencies AND expand them to base sources
        try:
            deps_expanded = set(result_output.dependencies or [])
            # Collect all temp tables found in dependencies
            temp_tables_found = set()
            for d in list(deps_expanded):
                low = str(d).lower()
                is_temp = ('#' in d) or ('tempdb' in low)
                if is_temp:
                    if '#' in d:
                        tname = d.split('#', 1)[1]
                        tname = tname.split('.')[0]
                        tkey = f"#{tname}"
                    else:
                        simple = d.split('.')[-1]
                        tkey = f"#{simple}"
                    # Keep the temp table in dependencies
                    temp_tables_found.add(tkey)
                    bases = set(self.temp_sources.get(tkey, set()))
                    if bases:
                        deps_expanded.update(bases)
            # Add all temp tables from all_outputs to dependencies (with canonical names)
            for obj in all_outputs:
                if obj and obj.name and (obj.name.startswith("#") or "tempdb" in obj.name.lower()):
                    # Extract simple temp name
                    if '#' in obj.name:
                        tname = obj.name.split('#', 1)[1]
                        tname = tname.split('.')[0]
                        tkey = f"#{tname}"
                    else:
                        tkey = f"#{obj.name.split('.')[-1]}"
                    temp_tables_found.add(tkey)
                    # Use canonical temp table name if available
                    ctx_db = getattr(self, '_ctx_db', None) or self.current_database or self.default_database
                    ctx_obj = getattr(self, '_ctx_obj', None) or (object_hint or "unknown_procedure")
                    if ctx_db and ctx_obj:
                        # Normalize procedure name for canonical temp naming
                        norm_proc = self._normalize_table_name_for_output(ctx_obj) if hasattr(self, '_normalize_table_name_for_output') else ctx_obj.split('.')[-1]
                        canonical_name = f"{ctx_db}.dbo.{norm_proc}#{tkey.lstrip('#')}"
                        deps_expanded.add(canonical_name)
                    else:
                        deps_expanded.add(tkey)
            # Also add temp tables from temp_registry that might not be in all_outputs
            # Skip table variables (starting with @) - they should not be materialized
            for tkey in self.temp_registry.keys():
                if tkey.startswith('#') and not tkey.startswith('@'):
                    temp_tables_found.add(tkey)
                    # Use canonical temp table name if available
                    ctx_db = getattr(self, '_ctx_db', None) or self.current_database or self.default_database
                    ctx_obj = getattr(self, '_ctx_obj', None) or (object_hint or "unknown_procedure")
                    if ctx_db and ctx_obj:
                        # Normalize procedure name for canonical temp naming
                        norm_proc = self._normalize_table_name_for_output(ctx_obj) if hasattr(self, '_normalize_table_name_for_output') else ctx_obj.split('.')[-1]
                        canonical_name = f"{ctx_db}.dbo.{norm_proc}#{tkey.lstrip('#')}"
                        deps_expanded.add(canonical_name)
                    else:
                        deps_expanded.add(tkey)
            result_output.dependencies = deps_expanded
        except Exception:
            pass

        # Last-resort: if deps still show only temp table(s), broaden using basic scan
        try:
            deps_now = set(result_output.dependencies or [])
            looks_like_only_temp = False
            if deps_now and all(('#' in d) or ('tempdb' in str(d).lower()) or (not d) for d in deps_now):
                looks_like_only_temp = True
            if not deps_now or looks_like_only_temp:
                broad = self._extract_basic_dependencies(sql_content) or set()
                if broad:
                    filt = {b for b in broad if re.match(r'^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$', str(b))}
                    result_output.dependencies = filt or broad
        except Exception:
            pass
        

        
        # DON'T restore context - it's needed for temp table canonical naming in engine.py Phase 3
        # The context will be restored when the next file is parsed (in parse_sql_file)
        # self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
        
        return result_output
    
    # Filter out known garbage dependencies that might have leaked from comments or partial parsing
    garbage_tokens = {'previous', 'end', 'desc', 'asc', 'case', 'when', 'then', 'else', 'select', 'from', 'where', 'group', 'by', 'order', 'having'}
    if all_inputs:
        all_inputs = {d for d in all_inputs if d.lower() not in garbage_tokens and not d.lower().endswith('.end') and not d.lower().endswith('.previous')}

    # Fallback: return basic procedure info with dependencies from all statements
    dependencies = all_inputs
    
    schema = TableSchema(
        namespace=namespace,
        name=sanitize_name(procedure_name),
        columns=[]
    )
    self.schema_registry.register(schema)
    
    obj = ObjectInfo(
        name=sanitize_name(procedure_name),
        object_type="procedure",
        schema=schema,
        lineage=[],
        dependencies=dependencies
    )
    obj.no_output_reason = "ONLY_PROCEDURE_RESULTSET"
    
    # DON'T restore context before returning - it's needed for temp table canonical naming in engine.py
    # The context will be restored when the next file is parsed (in parse_sql_file)
    logger.debug(f"_parse_procedure_body_statements: Before return, context: _ctx_db={getattr(self, '_ctx_db', None)}, _ctx_obj={getattr(self, '_ctx_obj', None)}")
    
    for k, v in self.temp_sources.items():
        if "previous" in str(v):
            print(f"DEBUG: temp_sources[{k}] HAS previous: {v}")

    # self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
    return obj

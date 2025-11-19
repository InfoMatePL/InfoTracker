from __future__ import annotations
from typing import Optional, List, Set
import re

import sqlglot
from sqlglot import exp  # type: ignore

from ..models import ObjectInfo, TableSchema, ColumnSchema, ColumnLineage


def _parse_procedure_string(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
    """Parse CREATE PROCEDURE using string-based approach (extracted)."""
    # Normalize headers (SET/GO, COLLATE, etc.)
    sql_content = self._normalize_tsql(sql_content)

    # Determine DB context from USE at the start
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
    prev_ctx_db, prev_ctx_obj = getattr(self, "_ctx_db", None), getattr(self, "_ctx_obj", None)
    self._ctx_db = inferred_db or self.current_database or self.default_database
    self._ctx_obj = self._normalize_table_name_for_output(procedure_name)

    # --- Prescan AST for temp materializations to register temp lineage early ---
    try:
        # Use the same preprocessing pipeline as the main parser so sqlglot can handle T-SQL procs
        normalized = self._normalize_tsql(sql_content)
        preprocessed = self._preprocess_sql(normalized)
        stmts = sqlglot.parse(preprocessed, read=self.dialect) or []
        for st in stmts:
            # Top-level SELECT ... INTO #tmp
            if isinstance(st, exp.Select) and self._is_select_into(st):
                self._parse_select_into(st, object_hint)
            # Top-level INSERT INTO #tmp SELECT ...
            if isinstance(st, exp.Insert):
                try:
                    if hasattr(st, 'expression') and isinstance(st.expression, exp.Select):
                        self._parse_insert_select(st, object_hint)
                except Exception:
                    pass
            if isinstance(st, exp.Create):
                for node in st.walk():
                    # Nested SELECT ... INTO #tmp inside CREATE PROCEDURE body
                    if isinstance(node, exp.Select) and self._is_select_into(node):
                        self._parse_select_into(node, object_hint)
                    # Nested INSERT INTO #tmp SELECT ... inside CREATE PROCEDURE body
                    if isinstance(node, exp.Insert):
                        try:
                            if hasattr(node, 'expression') and isinstance(node.expression, exp.Select):
                                self._parse_insert_select(node, object_hint)
                        except Exception:
                            pass
    except Exception:
        pass

    # 1) Check if procedure materializes (SELECT INTO / INSERT INTO ... SELECT)
    materialized_output = self._extract_materialized_output_from_procedure_string(sql_content)
    if materialized_output:
        # Specialized parser: INSERT INTO ... SELECT -> compute lineage from that SELECT
        try:
            ins_lineage, ins_deps = self._extract_insert_select_lineage_string(sql_content, procedure_name)
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
            materialized_output.schema = known
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
            for m in _re.finditer(r"(?is)\bSELECT\s+.*?\bINTO\s+#(?P<tmp>[A-Za-z0-9_]+)\b.*?(?=;|\bINSERT\b|\bCREATE\b|\bALTER\b|\bUPDATE\b|\bDELETE\b|\bEND\b|\bGO\b|$)", src_text):
                # Try AST on the normalized/preprocessed segment
                raw_seg = m.group(0)
                seg = self._preprocess_sql(self._normalize_tsql(raw_seg))
                try:
                    import sqlglot
                    from sqlglot import expressions as _exp
                    st = sqlglot.parse_one(seg, read=self.dialect)
                    if isinstance(st, _exp.Select):
                        # registers temp_registry/temp_sources/temp_lineage
                        self._parse_select_into(st, object_hint)
                except Exception:
                    # Fallback: approximate base deps from string scan
                    try:
                        tmp = m.group('tmp')
                        if tmp:
                            tkey = f"#{tmp}"
                            bases = self._extract_basic_dependencies(raw_seg) or set()
                            # Filter out self and temps
                            bases = {b for b in bases if '#' not in b and 'tempdb' not in str(b).lower()}
                            if bases:
                                self.temp_sources[tkey] = set(bases)
                    except Exception:
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
                                self.temp_sources[tkey] = set(bases)
                    except Exception:
                        pass
        except Exception:
            pass

        # Expand temp dependencies on the final output, if any
        try:
            deps_expanded = set(materialized_output.dependencies or [])
            # String-derived temp base map: #temp -> base deps
            temp_base_map: dict[str, Set[str]] = {}
            try:
                import re as _re
                for m2 in _re.finditer(r"(?is)\bSELECT\s+.*?\bINTO\s+#(?P<tmp>[A-Za-z0-9_]+)\b.*?(?=;|\bINSERT\b|\bCREATE\b|\bALTER\b|\bUPDATE\b|\bDELETE\b|\bEND\b|\bGO\b|$)", src_text):
                    raw_seg2 = m2.group(0)
                    tname2 = m2.group('tmp')
                    if tname2:
                        bases2 = self._extract_basic_dependencies(raw_seg2) or set()
                        temp_base_map[f"#{tname2}"] = {b for b in bases2 if '#' not in b and 'tempdb' not in str(b).lower()}
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
                bases = set(self.temp_sources.get(tkey, set()))
                if not bases and tkey in temp_base_map:
                    bases = set(temp_base_map[tkey])
                if bases:
                    deps_expanded.update(bases)
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
        # restore context before returning
        self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
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
    import re
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
    self._ctx_obj = procedure_name
    
    # Preprocess body (remove DECLARE, SET, etc.)
    preprocessed_body = self._preprocess_sql(body_sql)
    
    # Register temp tables found in procedure body for proper namespace resolution
    # sqlglot drops '#' from temp table names, so we need to register them beforehand
    import re
    temp_pattern = r'#(\w+)'
    for match in re.finditer(temp_pattern, preprocessed_body):
        temp_name = f"#{match.group(1)}"
        if temp_name not in self.temp_registry:
            # Register with placeholder schema (will be filled during actual INSERT parsing)
            self.temp_registry[temp_name] = []
    
    # Parse statements in body - use one_statement mode to be more forgiving
    try:
        statements = []
        # Split by semicolon but also try to extract major DML statements
        import sqlglot
        import re
        
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
        for stmt_sql in expanded_chunks:
            stmt_sql = stmt_sql.strip()
            if not stmt_sql or stmt_sql.upper() in ('GO', 'END'):
                continue
            try:
                parsed = sqlglot.parse_one(stmt_sql, read=self.dialect)
                if parsed:
                    statements.append(parsed)
            except Exception:
                # Skip unparseable statements
                continue
    except Exception:
        statements = []
    
    all_outputs = []
    all_inputs: Set[str] = set()
    last_persistent_output = None
    best_match_output = None
    
    for statement in statements:
        if isinstance(statement, exp.Insert):
            if self._is_insert_exec(statement):
                obj = self._parse_insert_exec(statement, object_hint)
            else:
                obj = self._parse_insert_select(statement, object_hint)
            
            if obj:
                all_outputs.append(obj)
                # Skip temp tables
                if obj.name.startswith("#") or "tempdb" in obj.name.lower():
                    all_inputs.update(obj.dependencies or [])
                    continue
                
                # Skip auxiliary tables (_ins_upd_results, _temp, etc.)
                table_basename = obj.name.split('.')[-1].lower()
                if any(suffix in table_basename for suffix in ['_ins_upd_results', '_results', '_log', '_audit']):
                    all_inputs.update(obj.dependencies or [])
                    continue
                
                # This is a candidate persistent table
                last_persistent_output = obj
                
                # If table name matches procedure name pattern (e.g., update_X_BV -> X_BV),
                # it's likely the main target
                proc_basename = object_hint.split('.')[-1].lower() if object_hint else ""
                if proc_basename.startswith('update_') or proc_basename.startswith('load_'):
                    expected_table = proc_basename.replace('update_', '').replace('load_', '')
                    if table_basename == expected_table or table_basename.endswith(expected_table):
                        best_match_output = obj
                
                all_inputs.update(obj.dependencies or [])
    
    # Prefer best match (procedure name → table name), otherwise use last persistent
    result_output = best_match_output or last_persistent_output
    
    # If we found persistent outputs, return the best one
    if result_output:
        # Restore context
        self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
        return result_output
    
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
    
    # Restore context
    self._ctx_db, self._ctx_obj = prev_ctx_db, prev_ctx_obj
    return obj

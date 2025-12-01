from __future__ import annotations

import re
import logging
import sqlglot
from sqlglot import expressions as exp
from typing import List, Set, Optional, Dict
from ..models import ColumnLineage, ColumnSchema, ColumnReference, TransformationType, ObjectInfo, TableSchema

logger = logging.getLogger(__name__)


def _find_last_select_string(self, sql_content: str, dialect: str = "tsql") -> str | None:
    try:
        normalized = self._normalize_tsql(sql_content)
        preprocessed = self._preprocess_sql(normalized)
        parsed = sqlglot.parse(preprocessed, read=self.dialect)
        selects = []
        for stmt in parsed:
            selects.extend(list(stmt.find_all(exp.Select)))
        if not selects:
            return None
        return str(selects[-1])
    except Exception:
        return _find_last_select_string_fallback(self, sql_content)


def _find_last_select_string_fallback(self, sql_content: str) -> str | None:
    try:
        select_positions = [m.start() for m in re.finditer(r"\bSELECT\b", sql_content, re.IGNORECASE)]
        if not select_positions:
            return None
        last_select_pos = select_positions[-1]
        remaining_content = sql_content[last_select_pos:]
        end_pattern = r'(?i)(?:^|\n)\s*END\s*(?:;|\s*$)'
        end_match = re.search(end_pattern, remaining_content)
        if end_match:
            last_select = remaining_content[:end_match.start()].strip()
        else:
            last_select = remaining_content.strip()
        last_select = re.sub(r';\s*$', '', last_select)
        return last_select
    except Exception as e:
        try:
            self._log_debug(f"Fallback SELECT extraction failed: {e}")
        except Exception:
            pass
        return None


def _extract_insert_select_lineage_string(self, sql_content: str, object_name: str) -> tuple[List[ColumnLineage], Set[str]]:
    """Extract column lineage from INSERT INTO ... SELECT statements.
    
    Searches for ALL INSERT INTO patterns and matches against object_name to handle
    procedures with multiple INSERT statements (temp tables + persistent tables).
    """
    import logging
    logger = logging.getLogger(__name__)
    lineage: List[ColumnLineage] = []
    dependencies: Set[str] = set()
    s = self._strip_sql_comments(self._normalize_tsql(sql_content))
    
    # Normalize object_name for comparison (strip schema/db prefix if present)
    # Remove square brackets from target
    target_simple = object_name.split('.')[-1].lower().strip('[]')
    # target_simple might contain procedure prefix like "update_asefl_TrialBalance_BV"
    # We need to extract just the table name part for comparison
    # Try to find the actual table name by checking if target_simple ends with a known table pattern
    # For now, we'll use a flexible matching approach: check if table_simple matches the end of target_simple
    
    logger.debug(f"_extract_insert_select_lineage_string: Looking for INSERT INTO target={target_simple}, object_name={object_name}")
    logger.debug(f"_extract_insert_select_lineage_string: SQL (first 500 chars): {sql_content[:500]}")
    
    # Find ALL INSERT INTO ... SELECT patterns (using finditer instead of search)
    pattern_with_terminator = r'(?is)INSERT\s+INTO\s+([^\s(]+)(?:\s*\([^)]*\))?\s+(?:OUTPUT[^;]*?)?\s*SELECT\b(.*?)(?:;|(?=\b(?:COMMIT|ROLLBACK|RETURN|END|GO|CREATE|ALTER|MERGE|UPDATE|DELETE|INSERT)\b)|$)'
    
    matches = list(re.finditer(pattern_with_terminator, s))
    logger.debug(f"_extract_insert_select_lineage_string: Found {len(matches)} INSERT INTO ... SELECT patterns")
    
    for match_idx, match in enumerate(matches):
        table_ref = match.group(1).strip()
        select_part = match.group(2)
        
        # Normalize table reference for comparison
        table_simple = table_ref.split('.')[-1].lower().strip('[]')
        logger.debug(f"_extract_insert_select_lineage_string: Match {match_idx+1}: table_ref={table_ref}, table_simple={table_simple}, target_simple={target_simple}")
        
        # Skip temp tables (unless target is also temp)
        if table_simple.startswith('#') and not target_simple.startswith('#'):
            logger.debug(f"_extract_insert_select_lineage_string: Skipping temp table {table_simple}")
            continue
        
        # Check if this INSERT matches our target table
        # Try exact match first, then check if table_simple is at the end of target_simple
        # (to handle cases where target_simple has procedure prefix like "update_asefl_TrialBalance_BV")
        # Since we now use raw_target (actual table name from SQL), exact match should work in most cases
        # Also handle cases where target_simple might be missing parts (e.g., "update_asefl_accountbalance_bv" vs "accountbalance_lnk_bv")
        # or where table names differ slightly (e.g., "AccountBalance_LNK_BV" vs "AccountBalance_BV")
        matches_target = (table_simple == target_simple or 
                         target_simple.endswith('_' + table_simple) or
                         target_simple.endswith(table_simple) or
                         # Check if table_simple is contained in target_simple (for cases like "update_asefl_accountbalance_bv" containing "accountbalance_lnk_bv")
                         (table_simple in target_simple and len(table_simple) > 5) or
                         # Check if they share a common suffix (for cases like "accountbalance_lnk_bv" vs "accountbalance_bv")
                         (len(table_simple) > 5 and len(target_simple) > 5 and 
                          (table_simple[-5:] in target_simple or target_simple[-5:] in table_simple)))
        if not matches_target:
            logger.debug(f"_extract_insert_select_lineage_string: Table {table_simple} doesn't match target {target_simple}, skipping")
            continue
        
        logger.debug(f"_extract_insert_select_lineage_string: Found matching INSERT INTO {table_simple}, extracting lineage from SELECT (first 200 chars): {select_part[:200]}")
        
        # Found matching INSERT - extract lineage from SELECT
        # Check if there's a WITH clause before this specific INSERT (CTE defined before INSERT INTO)
        # Find the position of this specific INSERT INTO in the original SQL
        insert_pos = match.start()
        select_sql = None
        cte_sql = None
        cte_name = None
        if insert_pos > 0:
            # Look for WITH clause before this INSERT, but only within reasonable distance (5000 chars)
            search_start = max(0, insert_pos - 5000)
            before_insert = sql_content[search_start:insert_pos]
            # Look for WITH clause that might be used in the SELECT (e.g., HashesCalculated)
            # Try to find the most recent WITH clause before this INSERT
            with_pattern = r'(?is)(WITH\s+(\w+)\s+AS\s*\()'
            with_matches = list(re.finditer(with_pattern, before_insert))
            if with_matches:
                # Use the last (most recent) WITH clause before this INSERT
                with_match = with_matches[-1]
                cte_name = with_match.group(2)
                # Check if the SELECT uses this CTE (check if CTE name appears in FROM clause)
                if cte_name.lower() in select_part.lower():
                    # Extract the full CTE definition by finding matching closing parenthesis
                    cte_start = with_match.start() + search_start
                    # Find the matching closing parenthesis
                    paren_count = 0
                    cte_end = cte_start
                    for i, char in enumerate(sql_content[cte_start:], start=cte_start):
                        if char == '(':
                            paren_count += 1
                        elif char == ')':
                            paren_count -= 1
                            if paren_count == 0:
                                cte_end = i + 1
                                break
                    if cte_end > cte_start:
                        # Extract full CTE SQL and combine with SELECT to create valid SQL
                        cte_sql = sql_content[cte_start:cte_end]
                        # Combine CTE with SELECT to create valid SQL that can be parsed
                        select_sql = cte_sql + " SELECT " + select_part
                        logger.debug(f"_extract_insert_select_lineage_string: Found WITH clause {cte_name} before INSERT, combining with SELECT")
        
        # If no CTE found, use just SELECT
        if not select_sql:
            select_sql = "SELECT " + select_part
        
        logger.debug(f"_extract_insert_select_lineage_string: About to parse SQL (first 300 chars): {select_sql[:300]}")
        try:
            parsed = sqlglot.parse(select_sql, read=self.dialect)
            logger.debug(f"_extract_insert_select_lineage_string: Parsed result: {parsed is not None}, length: {len(parsed) if parsed else 0}")
            # Handle both exp.Select and exp.With (WITH ... SELECT)
            select_stmt = None
            if parsed and len(parsed) > 0:
                logger.debug(f"_extract_insert_select_lineage_string: Parsed statement type: {type(parsed[0]).__name__}")
                if isinstance(parsed[0], exp.With) and hasattr(parsed[0], 'this'):
                    select_stmt = parsed[0].this
                    logger.debug(f"_extract_insert_select_lineage_string: Parsed as exp.With, has expressions: {hasattr(parsed[0], 'expressions')}")
                    # Process CTEs from exp.With - CTEs are in exp.With, not in exp.Select
                    # We need to manually extract CTEs from exp.With and register them
                    if hasattr(parsed[0], 'expressions'):
                        logger.debug(f"_extract_insert_select_lineage_string: Processing {len(parsed[0].expressions)} CTEs from exp.With")
                        for cte in parsed[0].expressions:
                            if hasattr(cte, 'alias') and hasattr(cte, 'this'):
                                cte_name = str(cte.alias)
                                cte_columns = []
                                if isinstance(cte.this, exp.Select):
                                    for proj in cte.this.expressions:
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
                                if isinstance(cte.this, exp.Select):
                                    self.cte_registry[cte_name] = {
                                        'columns': cte_columns,
                                        'definition': cte.this
                                    }
                                else:
                                    self.cte_registry[cte_name] = cte_columns
                    logger.debug(f"_extract_insert_select_lineage_string: Processed CTEs from exp.With, cte_registry keys: {list(self.cte_registry.keys())}")
                elif isinstance(parsed[0], exp.Select):
                    select_stmt = parsed[0]
                    # Process CTEs if present
                    with_clause = select_stmt.args.get('with')
                    logger.debug(f"_extract_insert_select_lineage_string: exp.Select has with clause: {with_clause is not None}")
                    if with_clause:
                        self._process_ctes(select_stmt)
                        logger.debug(f"_extract_insert_select_lineage_string: Processed CTEs from SELECT WITH, cte_registry keys: {list(self.cte_registry.keys())}")
                    else:
                        # If SELECT doesn't have with clause but we combined CTE with SELECT,
                        # SQLGlot might have parsed it incorrectly. Manually parse and register CTE
                        if cte_sql and cte_name:
                            logger.debug(f"_extract_insert_select_lineage_string: SELECT has no with clause, but we have CTE SQL - manually parsing CTE: {cte_name}")
                            try:
                                # Parse the full WITH ... AS (...) SELECT ... to extract CTE definition
                                # SQLGlot requires SELECT after CTE, so we need to parse the full statement
                                full_sql = cte_sql + " SELECT 1"  # Dummy SELECT to make it valid
                                cte_parsed = sqlglot.parse_one(full_sql, read=self.dialect)
                                logger.debug(f"_extract_insert_select_lineage_string: CTE parsed, type: {type(cte_parsed).__name__ if cte_parsed else 'None'}")
                                if isinstance(cte_parsed, exp.With) and hasattr(cte_parsed, 'expressions'):
                                    # Extract CTE from exp.With
                                    for cte in cte_parsed.expressions:
                                        if hasattr(cte, 'alias') and hasattr(cte, 'this'):
                                            parsed_cte_name = str(cte.alias)
                                            if parsed_cte_name.lower() == cte_name.lower():
                                                cte_columns = []
                                                if isinstance(cte.this, exp.Select):
                                                    for proj in cte.this.expressions:
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
                                                if isinstance(cte.this, exp.Select):
                                                    self.cte_registry[cte_name] = {
                                                        'columns': cte_columns,
                                                        'definition': cte.this
                                                    }
                                                else:
                                                    self.cte_registry[cte_name] = cte_columns
                                                logger.debug(f"_extract_insert_select_lineage_string: Registered CTE {cte_name} with {len(cte_columns)} columns")
                                                break
                                elif isinstance(cte_parsed, exp.Select):
                                    # If CTE was parsed as SELECT, try to extract columns from it
                                    cte_columns = []
                                    for proj in cte_parsed.expressions:
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
                                        'definition': cte_parsed
                                    }
                                    logger.debug(f"_extract_insert_select_lineage_string: Registered CTE {cte_name} (parsed as SELECT) with {len(cte_columns)} columns")
                                logger.debug(f"_extract_insert_select_lineage_string: CTE registry keys after manual parsing: {list(self.cte_registry.keys())}")
                            except Exception as e:
                                import traceback
                                logger.debug(f"_extract_insert_select_lineage_string: Failed to parse CTE manually: {e}, traceback: {traceback.format_exc()}")
            if select_stmt:
                logger.debug(f"_extract_insert_select_lineage_string: Successfully parsed SELECT, extracting lineage")
                lineage, _out_cols = self._extract_column_lineage(select_stmt, object_name)
                deps = self._extract_dependencies(select_stmt)
                dependencies.update(deps)
                logger.debug(f"_extract_insert_select_lineage_string: Extracted {len(lineage)} columns, {len(deps)} dependencies")
                break  # Found target, stop searching
            else:
                logger.debug(f"_extract_insert_select_lineage_string: Failed to parse SELECT as exp.Select, using basic dependencies")
                dependencies.update(self._extract_basic_dependencies(select_sql))
                break
        except Exception as parse_error:
            logger.debug(f"_extract_insert_select_lineage_string: Exception parsing SELECT: {parse_error}, using basic dependencies")
            dependencies.update(self._extract_basic_dependencies(select_sql))
            break
    
    logger.debug(f"_extract_insert_select_lineage_string: Returning {len(lineage)} columns, {len(dependencies)} dependencies")
    return lineage, dependencies


def _extract_materialized_output_from_procedure_string(self, sql_content: str) -> List[ObjectInfo]:
    """Extract ALL materialized outputs from procedure (SELECT INTO, INSERT INTO, TRUNCATE).
    
    Returns a list of ObjectInfo for all persistent tables modified by the procedure.
    """
    logger.debug(f"_extract_materialized_output_from_procedure_string: Called")
    outputs: List[ObjectInfo] = []
    s = self._normalize_tsql(sql_content)
    s = re.sub(r'/\*.*?\*/', '', s, flags=re.S)
    lines = s.splitlines()
    s = "\n".join(line for line in lines if not line.lstrip().startswith('--'))

    def _to_obj(table_token: str) -> Optional[ObjectInfo]:
        tok = (table_token or "").strip().rstrip(';')
        if tok.startswith('#') or tok.lower().startswith('tempdb..#'):
            return None
        norm = self._normalize_table_ident(tok)
        full_name = self._get_full_table_name(norm)
        try:
            db, sch, tbl = self._split_fqn(full_name)
        except Exception:
            parts = full_name.split('.')
            if len(parts) == 3:
                db, sch, tbl = parts
            elif len(parts) == 2:
                db = (self.current_database or self.default_database or "InfoTrackerDW")
                sch, tbl = parts
                full_name = f"{db}.{sch}.{tbl}"
            else:
                db = (self.current_database or self.default_database or "InfoTrackerDW")
                sch = "dbo"
                tbl = parts[0]
                full_name = f"{db}.{sch}.{tbl}"
        canon_db = db or (self.current_database or self.default_database or 'InfoTrackerDW')
        ns = self._canonical_namespace(canon_db)
        return ObjectInfo(name=full_name, object_type="table", schema=TableSchema(namespace=ns, name=full_name, columns=[]), lineage=[], dependencies=set())

    # Collect all SELECT INTO outputs
    for m in re.finditer(r'(?is)\bSELECT\s+.*?\bINTO\s+([^\s,()\r\n;]+)', s):
        obj = _to_obj(m.group(1))
        if obj:
            # Try to extract lineage for SELECT INTO
            try:
                select_into_sql = s[max(0, m.start()-1000):m.end()+5000]  # Get context around SELECT INTO
                lineage, deps = self._extract_insert_select_lineage_string(select_into_sql, obj.name)
                if lineage:
                    obj.lineage = lineage
                if deps:
                    obj.dependencies = deps
            except Exception as e:
                logger.debug(f"_extract_materialized_output_from_procedure_string: Failed to extract lineage for SELECT INTO {obj.name}: {e}")
            outputs.append(obj)
    # Collect all INSERT INTO outputs (non-temp tables)
    for m in re.finditer(r'(?is)\bINSERT\s+INTO\s+([^\s,()\r\n;]+)', s):
        table_token = m.group(1).strip().rstrip(';')
        # Skip temp tables
        if table_token.startswith('#') or table_token.lower().startswith('tempdb..#'):
            continue
        obj = _to_obj(table_token)
        if obj:
            # Try to extract lineage for INSERT INTO
            try:
                # Extract the full INSERT INTO ... SELECT statement
                insert_start = m.start()
                insert_end = s.find(';', insert_start)
                if insert_end == -1:
                    insert_end = len(s)
                insert_sql = s[insert_start:insert_end]
                # Use the actual table name from SQL, not the normalized one
                table_name_from_sql = table_token.strip().strip('[]')
                # Remove square brackets properly
                table_name_clean = re.sub(r'\[([^\]]+)\]', r'\1', table_name_from_sql)
                logger.debug(f"_extract_materialized_output_from_procedure_string: Extracting lineage for INSERT INTO {table_name_clean}, obj.name={obj.name}")
                lineage, deps = self._extract_insert_select_lineage_string(insert_sql, table_name_clean)
                if lineage:
                    obj.lineage = lineage
                if deps:
                    obj.dependencies = deps
                logger.debug(f"_extract_materialized_output_from_procedure_string: Extracted {len(lineage)} columns, {len(deps)} dependencies for INSERT INTO {table_name_clean}")
            except Exception as e:
                logger.debug(f"_extract_materialized_output_from_procedure_string: Failed to extract lineage for INSERT INTO {obj.name}: {e}")
            outputs.append(obj)
    
    # Collect TRUNCATE TABLE as output operation (table is modified)
    for m in re.finditer(r'(?is)\bTRUNCATE\s+TABLE\s+([^\s,()\r\n;]+)', s):
        table_token = m.group(1).strip().rstrip(';')
        # Skip temp tables
        if table_token.startswith('#') or table_token.lower().startswith('tempdb..#'):
            continue
        obj = _to_obj(table_token)
        if obj:
            logger.debug(f"_extract_materialized_output_from_procedure_string: Detected TRUNCATE TABLE {obj.name}")
            # Check if this table is already in outputs (from INSERT INTO)
            # If so, don't add it again
            if not any(o.name == obj.name for o in outputs):
                outputs.append(obj)
    
    logger.debug(f"_extract_materialized_output_from_procedure_string: Found {len(outputs)} outputs")
    return outputs


def _extract_first_create_statement(self, sql_content: str, statement_type: str) -> str:
    """Extract the first CREATE statement of the specified type (FUNCTION|PROCEDURE)."""
    patterns = {
        'FUNCTION': [
            r'CREATE\s+(?:OR\s+ALTER\s+)?FUNCTION\s+.*?(?=CREATE\s+(?:OR\s+ALTER\s+)?(?:FUNCTION|PROCEDURE)|$)',
            r'CREATE\s+FUNCTION\s+.*?(?=CREATE\s+(?:FUNCTION|PROCEDURE)|$)'
        ],
        'PROCEDURE': [
            r'CREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\s+.*?(?=CREATE\s+(?:OR\s+ALTER\s+)?(?:FUNCTION|PROCEDURE)|$)',
            r'CREATE\s+PROCEDURE\s+.*?(?=CREATE\s+(?:FUNCTION|PROCEDURE)|$)'
        ]
    }
    if statement_type not in patterns:
        return ""
    for pattern in patterns[statement_type]:
        match = re.search(pattern, sql_content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(0).strip()
    return ""


def _try_insert_exec_fallback(self, sql_content: str, object_hint: Optional[str] = None) -> Optional[ObjectInfo]:
    """Best-effort fallback for INSERT EXEC and INSERT INTO table patterns when AST parsing fails."""
    from ..openlineage_utils import sanitize_name

    sql_pre = self._preprocess_sql(sql_content)
    insert_exec_pattern = r'(?is)INSERT\s+INTO\s+([#\[\]\w.]+)\s+EXEC\s+([^\s(;]+)'
    exec_match = re.search(insert_exec_pattern, sql_pre)
    insert_table_pattern = r'(?is)INSERT\s+INTO\s+([^\s#][#\[\]\w.]+)\s*\(([^)]+)\)\s+SELECT'
    table_match = re.search(insert_table_pattern, sql_pre)
    all_dependencies = self._extract_basic_dependencies(sql_pre)

    placeholder_columns = [ColumnSchema(name="output_col_1", data_type="unknown", nullable=True, ordinal=0)]
    namespace = self._canonical_namespace(self.current_database or self.default_database or 'InfoTrackerDW')
    table_name = sanitize_name(object_hint or "script_output")
    object_type = "script"

    if table_match and not table_match.group(1).startswith('#'):
        raw_table = table_match.group(1)
        raw_columns = table_match.group(2)
        table_name_norm = self._normalize_table_ident(raw_table)
        ns, nm = self._ns_and_name(table_name_norm)
        namespace, table_name, object_type = ns, nm, "table"
        placeholder_columns = [ColumnSchema(name=col.strip(), data_type="unknown", nullable=True, ordinal=i) for i, col in enumerate(raw_columns.split(','))]
    elif exec_match:
        raw_table = exec_match.group(1)
        raw_proc = exec_match.group(2)
        table_name_norm = self._normalize_table_ident(raw_table)
        proc_name = self._clean_proc_name(raw_proc)
        if table_name_norm.startswith('#'):
            temp_name = table_name_norm.lstrip('#')
            db = self.current_database or self.default_database or "InfoTrackerDW"
            sch = getattr(self, 'default_schema', None) or "dbo"
            label = (object_hint or "object")
            table_name = f"{db}.{sch}.{label}.#{temp_name}"
            namespace = self._canonical_namespace(db)
            object_type = "temp_table"
        else:
            table_name = self._get_full_table_name(table_name_norm)
            ns, nm = self._ns_and_name(table_name)
            namespace, table_name, object_type = ns, nm, "table"
        proc_full_name = sanitize_name(self._get_full_table_name(proc_name))
        all_dependencies.add(proc_full_name)
    else:
        if not all_dependencies:
            return None

    schema = TableSchema(namespace=namespace, name=table_name, columns=placeholder_columns)
    lineage: List[ColumnLineage] = []

    if table_match and not table_match.group(1).startswith('#') and placeholder_columns:
        proc_pattern = r'(?is)INSERT\s+INTO\s+#\w+\s+EXEC\s+([^\s(;]+)'
        proc_match = re.search(proc_pattern, sql_pre)
        if proc_match:
            proc_full_name = sanitize_name(self._get_full_table_name(self._clean_proc_name(proc_match.group(1))))
            ns_p, nm_p = self._ns_and_name(proc_full_name)
            for col in placeholder_columns:
                if col.name.lower() in ['archivedate', 'createdate', 'insertdate'] and 'getdate' in sql_pre.lower():
                    lineage.append(ColumnLineage(output_column=col.name, input_fields=[], transformation_type=TransformationType.CONSTANT, transformation_description="GETDATE() constant value for archiving"))
                else:
                    lineage.append(ColumnLineage(output_column=col.name, input_fields=[ColumnReference(namespace=ns_p, table_name=nm_p, column_name=col.name)], transformation_type=TransformationType.IDENTITY, transformation_description=f"{col.name} from procedure output via temp table"))
        else:
            for col in placeholder_columns:
                lineage.append(ColumnLineage(output_column=col.name, input_fields=[], transformation_type=TransformationType.UNKNOWN, transformation_description=f"Column {col.name} from complex transformation"))
    elif exec_match:
        proc_full_name = sanitize_name(self._get_full_table_name(self._clean_proc_name(exec_match.group(2))))
        ns_p, nm_p = self._ns_and_name(proc_full_name)
        for col in placeholder_columns:
            # Use schema.proc in description (without DB prefix) to match tests
            lineage.append(
                ColumnLineage(
                    output_column=col.name,
                    input_fields=[ColumnReference(namespace=ns_p, table_name=nm_p, column_name="*")],
                    transformation_type=TransformationType.EXEC,
                    transformation_description=f"INSERT INTO {table_name} EXEC {nm_p}"
                )
            )

    self.schema_registry.register(schema)
    obj = ObjectInfo(name=table_name, object_type=object_type, schema=schema, lineage=lineage, dependencies=all_dependencies, is_fallback=True)
    try:
        if getattr(self, 'dbt_mode', False) and object_hint:
            obj.job_name = f"dbt/models/{object_hint}.sql"
    except Exception:
        pass
    return obj


def _extract_table_variable_schema_string(self, sql_content: str) -> List[ColumnSchema]:
    """Extract column schema from @table TABLE definition using regex (string fallback)."""
    output_columns: List[ColumnSchema] = []

    match = re.search(r'@\w+\s+TABLE\s*\((.*?)\)', sql_content, re.IGNORECASE | re.DOTALL)
    if match:
        columns_def = match.group(1)
        for i, col_def in enumerate(columns_def.split(',')):
            col_def = col_def.strip()
            if not col_def:
                continue
            parts = col_def.split()
            if len(parts) >= 2:
                col_name = parts[0].strip()
                col_type = parts[1].strip()
                output_columns.append(ColumnSchema(
                    name=col_name,
                    data_type=col_type,
                    nullable=True,
                    ordinal=i,
                ))
    return output_columns


def _extract_basic_select_columns(self, select_sql: str) -> List[ColumnSchema]:
    """Basic extraction of column names from a SELECT (string parsing)."""
    output_columns: List[ColumnSchema] = []

    match = re.search(r'SELECT\s+(.*?)\s+FROM', select_sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return output_columns

    select_list = match.group(1)
    columns = [col.strip() for col in select_list.split(',')]
    for i, col in enumerate(columns):
        if ' AS ' in col.upper():
            col_name = col.split(' AS ')[-1].strip()
        elif ' ' in col and not any(func in col.upper() for func in ['SUM', 'COUNT', 'MAX', 'MIN', 'AVG', 'CAST', 'CASE']):
            parts = col.strip().split()
            col_name = parts[-1]
        else:
            # Derive a stable name by stripping expression tails if no alias
            try:
                from .select_lineage import _strip_expr_tail
                col_name = _strip_expr_tail(col)
            except Exception:
                col_name = col.split('.')[-1] if '.' in col else col
                col_name = re.sub(r'[^\w]', '', col_name)

        if col_name:
            output_columns.append(ColumnSchema(
                name=col_name,
                data_type="varchar",
                nullable=True,
                ordinal=i,
            ))

    return output_columns


def _extract_table_aliases_from_select(self, select_sql: str) -> Dict[str, str]:
    """Extract table aliases from FROM and JOIN clauses (string parsing)."""
    aliases: Dict[str, str] = {}
    from_join_pattern = r'(?i)\b(?:FROM|JOIN)\s+([^\s]+)(?:\s+AS\s+)?(\w+)?'
    matches = re.findall(from_join_pattern, select_sql)
    for table_name, alias in matches:
        clean_table = table_name.strip()
        clean_alias = alias.strip() if alias else None
        if clean_alias:
            aliases[clean_alias] = clean_table
        else:
            table_short = clean_table.split('.')[-1]
            aliases[table_short] = clean_table
    return aliases


def _parse_column_expression(self, col_expr: str, table_aliases: Dict[str, str]) -> tuple[str, str, TransformationType]:
    """Parse a SELECT expression to discover source table/column and transformation type (string parsing)."""
    col_expr = col_expr.strip()

    if ' AS ' in col_expr.upper():
        col_expr = col_expr.split(' AS ')[0].strip()
    elif ' ' in col_expr and not any(func in col_expr.upper() for func in ['SUM', 'COUNT', 'MAX', 'MIN', 'AVG', 'CAST', 'CASE']):
        parts = col_expr.split()
        if len(parts) > 1:
            col_expr = ' '.join(parts[:-1]).strip()

    if any(func in col_expr.upper() for func in ['SUM(', 'COUNT(', 'MAX(', 'MIN(', 'AVG(']):
        transformation_type = TransformationType.AGGREGATION
    elif 'CASE' in col_expr.upper():
        transformation_type = TransformationType.CONDITIONAL
    elif any(op in col_expr for op in ['+', '-', '*', '/']):
        transformation_type = TransformationType.ARITHMETIC
    else:
        transformation_type = TransformationType.IDENTITY

    col_match = re.search(r'(\w+)\.(\w+)', col_expr)
    if col_match:
        alias = col_match.group(1)
        column = col_match.group(2)
        if alias in table_aliases:
            table_name = table_aliases[alias]
            if not table_name.startswith('dbo.') and '.' not in table_name:
                table_name = f"dbo.{table_name}"
            return table_name, column, transformation_type

    simple_col_match = re.search(r'\b(\w+)\b', col_expr)
    if simple_col_match:
        column = simple_col_match.group(1)
        return "unknown_table", column, transformation_type

    return None, None, transformation_type


def _extract_basic_lineage_from_select(self, select_sql: str, output_columns: List[ColumnSchema], object_name: str) -> List[ColumnLineage]:
    """Basic lineage extraction from a SELECT using string parsing as fallback."""
    lineage: List[ColumnLineage] = []
    try:
        table_aliases = _extract_table_aliases_from_select(self, select_sql)
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', select_sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return lineage
        select_list = select_match.group(1)
        column_expressions = [col.strip() for col in select_list.split(',')]
        for output_col, col_expr in zip(output_columns, column_expressions):
            source_table, source_column, transformation_type = _parse_column_expression(self, col_expr, table_aliases)
            if source_table and source_column:
                lineage.append(ColumnLineage(
                    column_name=output_col.name,
                    table_name=object_name,
                    source_column=source_column,
                    source_table=source_table,
                    transformation_type=transformation_type,
                    transformation_description=f"Column derived from {source_table}.{source_column}",
                ))
    except Exception as e:
        try:
            self._log_debug(f"Basic lineage extraction failed: {e}")
        except Exception:
            pass
    return lineage


def _extract_update_from_lineage_string(self, sql_content: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str], Optional[str]]:
    lineage: List[ColumnLineage] = []
    output_columns: List[ColumnSchema] = []
    dependencies: Set[str] = set()
    target_table: Optional[str] = None
    s = self._strip_sql_comments(self._normalize_tsql(sql_content))
    m_upd = re.search(r'(?is)\bUPDATE\s+([^\s\(,;]+)(?:\s+AS\s+(\w+)|\s+(\w+))?\s+SET\s+(.*?)\bFROM\b(.*)$', s)
    if not m_upd:
        return lineage, output_columns, dependencies, None
    target_raw = self._normalize_table_ident(m_upd.group(1))
    tgt_alias = (m_upd.group(2) or m_upd.group(3) or '').strip() or None
    set_block = m_upd.group(4) or ''
    from_tail = m_upd.group(5) or ''
    parts = target_raw.split('.')
    if len(parts) >= 3:
        target_table = f"{parts[-2]}.{parts[-1]}"
    elif len(parts) == 2:
        target_table = target_raw
    else:
        target_table = f"dbo.{target_raw}"

    alias_map: Dict[str, str] = {}
    def _noise_token(tok: str) -> bool:
        t = (tok or '').strip()
        return (t.startswith('@') or ('+' in t) or (t.startswith('[') and t.endswith(']') and '.' not in t))
    for m in re.finditer(r'(?is)\bFROM\s+([^\s,;()]+)(?:\s+AS\s+(\w+)|\s+(\w+))?', ' ' + from_tail):
        raw_tok = m.group(1)
        if _noise_token(raw_tok):
            continue
        tbl = self._normalize_table_ident(raw_tok)
        # For temp tables, use _ns_and_name to get proper format with procedure context
        if tbl and (tbl.startswith('#') or 'tempdb' in tbl.lower()):
            _, tbl = self._ns_and_name(tbl, obj_type_hint="temp_table")
        al = (m.group(2) or m.group(3) or '').strip()
        if al:
            alias_map[al.lower()] = tbl
        else:
            alias_map[tbl.split('.')[-1].lower()] = tbl
    for m in re.finditer(r'(?is)\bJOIN\s+([^\s,;()]+)(?:\s+AS\s+(\w+)|\s+(\w+))?', from_tail):
        raw_tok = m.group(1)
        if _noise_token(raw_tok):
            continue
        tbl = self._normalize_table_ident(raw_tok)
        # For temp tables, use _ns_and_name to get proper format with procedure context
        if tbl and (tbl.startswith('#') or 'tempdb' in tbl.lower()):
            _, tbl = self._ns_and_name(tbl, obj_type_hint="temp_table")
        al = (m.group(2) or m.group(3) or '').strip()
        if al:
            alias_map[al.lower()] = tbl
        else:
            alias_map[tbl.split('.')[-1].lower()] = tbl
    default_src = None
    for al, tbl in alias_map.items():
        if not tgt_alias or al != tgt_alias.lower():
            default_src = tbl
            break
    def _tt(expr: str) -> TransformationType:
        e = expr.upper()
        if 'HASHBYTES' in e:
            return TransformationType.EXPRESSION
        if re.search(r'\bCAST\s*\(|\bCONVERT\s*\(|\bTRY_CAST\s*\(', e):
            return TransformationType.CAST
        if re.search(r'\bCOALESCE\s*\(|\bISNULL\s*\(', e):
            return TransformationType.EXPRESSION
        return TransformationType.IDENTITY
    assigns: List[tuple[str, str]] = []
    for a in re.split(r',\s*', set_block):
        a = a.strip()
        if not a:
            continue
        ma = re.search(r'(?is)(?:' + (re.escape(tgt_alias) + r'\.|' if tgt_alias else '') + r'\w+\.)?(\w+)\s*=\s*(.+)$', a)
        if not ma:
            continue
        assigns.append((ma.group(1), ma.group(2)))
    seen = set()
    for i, (t_col, _expr) in enumerate(assigns):
        if t_col not in seen:
            output_columns.append(ColumnSchema(name=t_col, data_type=None, nullable=True, ordinal=i))
            seen.add(t_col)
    for tbl in set(alias_map.values()):
        dependencies.add(tbl)
    for (t_col, expr) in assigns:
        refs: List[ColumnReference] = []
        for m in re.finditer(r'(?i)\b([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)\b', expr):
            al = m.group(1).lower()
            col = m.group(2)
            if tgt_alias and al == tgt_alias.lower():
                continue
            base = alias_map.get(al)
            if base:
                # base is already normalized (including temp tables with procedure context)
                # Extract namespace and name - if base already has schema.object#temp format, use it directly
                if base and (base.startswith('#') or 'tempdb' in base.lower()):
                    # This is a temp table that was already normalized in alias_map
                    ns, nm = self._ns_and_name(base, obj_type_hint="temp_table")
                else:
                    ns, nm = self._ns_and_name(base)
                refs.append(ColumnReference(namespace=ns, table_name=nm, column_name=col))
        if not refs and default_src:
            # default_src is already normalized (including temp tables with procedure context)
            if default_src and (default_src.startswith('#') or 'tempdb' in default_src.lower()):
                # This is a temp table that was already normalized in alias_map
                ns, nm = self._ns_and_name(default_src, obj_type_hint="temp_table")
            else:
                ns, nm = self._ns_and_name(default_src)
            mlast = re.search(r'(?i)\b([A-Za-z_][\w]*)\b$', expr)
            if mlast:
                refs.append(ColumnReference(namespace=ns, table_name=nm, column_name=mlast.group(1)))
        lineage.append(ColumnLineage(output_column=t_col, input_fields=refs, transformation_type=_tt(expr), transformation_description=f"UPDATE expr: {t_col} = {expr.strip()}"))
    return lineage, output_columns, dependencies, target_table


def _extract_output_into_lineage_string(self, sql_content: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str], Optional[str]]:
    lineage: List[ColumnLineage] = []
    output_columns: List[ColumnSchema] = []
    dependencies: Set[str] = set()
    target_output: Optional[str] = None
    s = self._strip_sql_comments(self._normalize_tsql(sql_content))
    def _st(name: str) -> str:
        name = self._normalize_table_ident(name)
        parts = name.split('.')
        if len(parts) >= 3:
            return f"{parts[-2]}.{parts[-1]}"
        if len(parts) == 2:
            return name
        return f"dbo.{name}"
    m_upd = re.search(r'(?is)\bUPDATE\s+([^\s(,;]+).*?\bOUTPUT\b\s+(.*?)\s+\bINTO\b\s+([^\s(,;]+)', s)
    dml_type = None
    out_exprs = None
    dml_target = None
    if m_upd:
        dml_type = 'UPDATE'
        dml_target = _st(m_upd.group(1))
        out_exprs = m_upd.group(2)
        target_output = _st(m_upd.group(3))
    else:
        m_ins = re.search(r'(?is)\bINSERT\s+INTO\s+([^\s(,;]+)[^;]*?\bOUTPUT\b\s+(.*?)\s+\bINTO\b\s+([^\s(,;]+)', s)
        if m_ins:
            dml_type = 'INSERT'
            dml_target = _st(m_ins.group(1))
            out_exprs = m_ins.group(2)
            target_output = _st(m_ins.group(3))
        else:
            m_del = re.search(r'(?is)\bDELETE\s+FROM\s+([^\s(,;]+).*?\bOUTPUT\b\s+(.*?)\s+\bINTO\b\s+([^\s(,;]+)', s)
            if m_del:
                dml_type = 'DELETE'
                dml_target = _st(m_del.group(1))
                out_exprs = m_del.group(2)
                target_output = _st(m_del.group(3))
    if not dml_type or not out_exprs or not target_output:
        return lineage, output_columns, dependencies, None
    dependencies.add(dml_target)
    alias_map: Dict[str, str] = {}
    if dml_type == 'UPDATE':
        m_from = re.search(r'(?is)\bFROM\b(.*)$', s)
        if m_from:
            from_tail = m_from.group(1)
            def _noise_token(tok: str) -> bool:
                t = (tok or '').strip()
                return (t.startswith('@') or ('+' in t) or (t.startswith('[') and t.endswith(']') and '.' not in t))
            for m in re.finditer(r'(?is)\bFROM\s+([^\s,;()]+)(?:\s+AS\s+(\w+)|\s+(\w+))?', ' ' + from_tail):
                raw_tok = m.group(1)
                if _noise_token(raw_tok):
                    continue
                tbl = self._normalize_table_ident(raw_tok)
                al = (m.group(2) or m.group(3) or '').strip()
                if al:
                    alias_map[al.lower()] = tbl
                else:
                    alias_map[tbl.split('.')[-1].lower()] = tbl
            for m in re.finditer(r'(?is)\bJOIN\s+([^\s,;()]+)(?:\s+AS\s+(\w+)|\s+(\w+))?', from_tail):
                raw_tok = m.group(1)
                if _noise_token(raw_tok):
                    continue
                tbl = self._normalize_table_ident(raw_tok)
                al = (m.group(2) or m.group(3) or '').strip()
                if al:
                    alias_map[al.lower()] = tbl
                else:
                    alias_map[tbl.split('.')[-1].lower()] = tbl
            for tbl in set(alias_map.values()):
                dependencies.add(tbl)
        # Resolve UPDATE target alias to real table if necessary
        parts = (dml_target or '').split('.')
        if len(parts) < 2 and dml_target:
            real = alias_map.get((dml_target or '').lower())
            if real:
                rparts = real.split('.')
                if len(rparts) >= 2:
                    dml_target = f"{rparts[-2]}.{rparts[-1]}"
                else:
                    dml_target = f"dbo.{real}"
    def _split_expr_list(t: str) -> List[str]:
        items = []
        depth = 0
        buf = []
        for ch in t:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            if ch == ',' and depth == 0:
                items.append(''.join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        if buf:
            items.append(''.join(buf).strip())
        return items
    exprs = _split_expr_list(out_exprs)
    out_names: List[str] = []
    for i, raw in enumerate(exprs):
        raw = raw.strip()
        out_name = None
        m_as = re.search(r'(?is)\bAS\s+(\w+)$', raw)
        if m_as:
            out_name = m_as.group(1)
        if not out_name:
            m_col = re.search(r'(?i)\b(?:inserted|deleted)\.\[?(\w+)\]?', raw)
            if m_col:
                out_name = m_col.group(1)
            else:
                try:
                    from .select_lineage import _strip_expr_tail
                    out_name = _strip_expr_tail(raw) or f"col_{i+1}"
                except Exception:
                    out_name = f"col_{i+1}"
        out_names.append(out_name)
        refs: List[ColumnReference] = []
        for m in re.finditer(r'(?i)\b([A-Za-z_][\w]*)\.\[?(\w+)\]?', raw):
            al = m.group(1).lower()
            col = m.group(2)
            if al in {'inserted','deleted'}:
                ns, nm = self._ns_and_name(dml_target)
                refs.append(ColumnReference(namespace=ns, table_name=nm, column_name=col))
                continue
            base = alias_map.get(al)
            if base:
                ns, nm = self._ns_and_name(base)
                refs.append(ColumnReference(namespace=ns, table_name=nm, column_name=col))
        output_columns.append(ColumnSchema(name=out_name, data_type=None, nullable=True, ordinal=i))
        lineage.append(ColumnLineage(output_column=out_name, input_fields=refs, transformation_type=TransformationType.EXPRESSION, transformation_description=f"OUTPUT expr: {raw}"))
    return lineage, output_columns, dependencies, target_output


def _extract_procedure_lineage_string(self, sql_content: str, procedure_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
    lineage: List[ColumnLineage] = []
    output_columns: List[ColumnSchema] = []
    dependencies: Set[str] = set()
    m = re.search(r'(?is)INSERT\s+INTO\s+[^\s(]+(?:\s*\([^)]*\))?\s+SELECT\b(.*)$', sql_content)
    if m:
        select_sql = "SELECT " + m.group(1)
        try:
            parsed = sqlglot.parse(select_sql, read=self.dialect)
            if parsed and isinstance(parsed[0], exp.Select):
                lineage, output_columns = self._extract_column_lineage(parsed[0], procedure_name)
                deps = self._extract_dependencies(parsed[0])
                dependencies.update(deps)
        except Exception:
            dependencies.update(self._extract_basic_dependencies(select_sql))

    last_select_sql = _find_last_select_string(self, sql_content)
    if last_select_sql:
        try:
            parsed = sqlglot.parse(last_select_sql, read=self.dialect)
            if parsed and isinstance(parsed[0], exp.Select):
                lineage, output_columns = self._extract_column_lineage(parsed[0], procedure_name)
                dependencies = self._extract_dependencies(parsed[0])
        except Exception:
            output_columns = self._extract_basic_select_columns(last_select_sql)
            lineage = self._extract_basic_lineage_from_select(last_select_sql, output_columns, procedure_name)
            dependencies = self._extract_basic_dependencies(last_select_sql)

    procedure_dependencies = self._extract_basic_dependencies(sql_content)
    dependencies.update(procedure_dependencies)
    return lineage, output_columns, dependencies


def _extract_tvf_lineage_string(self, sql_text: str, function_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
    lineage: List[ColumnLineage] = []
    output_columns: List[ColumnSchema] = []
    dependencies: Set[str] = set()
    select_string = self._extract_select_from_return_string(sql_text)
    if select_string:
        try:
            statements = sqlglot.parse(select_string, dialect=sqlglot.dialects.TSQL)
            if statements:
                select_stmt = statements[0]
                self._process_ctes(select_stmt)
                lineage, output_columns = self._extract_column_lineage(select_stmt, function_name)
                raw_deps = self._extract_dependencies(select_stmt)
                for dep in raw_deps:
                    expanded_deps = self._expand_dependency_to_base_tables(dep, select_stmt)
                    dependencies.update(expanded_deps)
        except Exception:
            basic_deps = self._extract_basic_dependencies(sql_text)
            dependencies.update(basic_deps)
    return lineage, output_columns, dependencies


def _extract_merge_lineage_string(self, sql_content: str, procedure_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str], Optional[str]]:
    """Parse MERGE INTO ... USING ... and try to build lineage (string-based)."""
    lineage: List[ColumnLineage] = []
    output_columns: List[ColumnSchema] = []
    dependencies: Set[str] = set()
    target_table: Optional[str] = None

    cleaned = self._strip_sql_comments(sql_content)
    # Target
    m_target = re.search(r'(?is)MERGE\s+INTO\s+([^\s\(,;]+)(?:\s+AS\s+(\w+)|\s+(\w+))?', cleaned)
    if not m_target:
        return lineage, output_columns, dependencies, None
    target_raw = self._normalize_table_ident(m_target.group(1))
    tgt_alias = (m_target.group(2) or m_target.group(3) or '').strip() or None
    parts = target_raw.split('.')
    if len(parts) >= 3:
        target_table = f"{parts[-2]}.{parts[-1]}"
    elif len(parts) == 2:
        target_table = target_raw
    else:
        target_table = f"dbo.{target_raw}"

    # Source
    m_using = re.search(r'(?is)USING\s+([^\s\(,;#]+|#\w+)(?:\s+AS\s+(\w+)|\s+(\w+))?', cleaned)
    source_name: Optional[str] = None
    src_alias: Optional[str] = None
    if m_using:
        src = m_using.group(1).strip()
        source_name = self._normalize_table_ident(src)
        src_alias = (m_using.group(2) or m_using.group(3) or '').strip() or None

    # Map temp -> base if created earlier in the body
    temp_to_base: Dict[str, str] = {}
    # Pattern 1: SELECT ... INTO #tmp FROM base
    for m in re.finditer(r'(?is)SELECT\s+.*?\s+INTO\s+(#\w+)\s+FROM\s+([^\s\(,;]+(?:\.[^\s\(,;]+)*)', cleaned):
        temp_to_base[self._normalize_table_ident(m.group(1))] = self._normalize_table_ident(m.group(2))
    # Pattern 2: INSERT INTO #tmp (...) SELECT ... FROM base
    for m in re.finditer(r'(?is)INSERT\s+INTO\s+(#\w+)\b.*?\bSELECT\b.*?\bFROM\s+([^\s\(,;]+(?:\.[^\s\(,;]+)*)', cleaned):
        temp_to_base[self._normalize_table_ident(m.group(1))] = self._normalize_table_ident(m.group(2))
    if source_name and (source_name.startswith('#') or source_name.lower().startswith('tempdb..#')):
        base = temp_to_base.get(source_name) or temp_to_base.get(source_name.split('.')[-1])
        if base:
            source_name = base

    # Parse SET assignments and build lineage
    assign_exprs: List[tuple[str, str]] = []
    m_when = re.search(r'(?is)WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+(.*?)\s*(?:WHEN\s+|OUTPUT\s+|;|$)', cleaned)
    if m_when:
        set_block = m_when.group(1)
        parts = re.split(r',(?![^\(]*\))', set_block)
        for p in parts:
            p = p.strip()
            if not p:
                continue
            # Split "t.col = expr"
            mm = re.match(r'(?is)([^=]+?)=\s*(.*)$', p)
            if not mm:
                continue
            left = mm.group(1).strip()
            right = mm.group(2).strip()
            # Left can be alias.col or schema.table.col
            mleft = re.search(r'(?is)(?:\w+\.|\[[^\]]+\]\.)?([\w\[\]]+)\.(\w+)$', left)
            if not mleft:
                continue
            l_alias_or_tbl = mleft.group(1).strip('[]')
            l_col = mleft.group(2)
            # If left refers to target alias, keep only the column part
            if tgt_alias and l_alias_or_tbl.lower() == tgt_alias.lower():
                assign_exprs.append((l_col, right))
            else:
                # Alternatively treat as target schema/table.col
                assign_exprs.append((l_col, right))

    # OUTPUT clause
    m_output = re.search(r'(?is)OUTPUT\s+(.*?)\s+INTO\s+([^\s,;]+)', cleaned)
    if m_output:
        out_exprs = m_output.group(1)
        out_target = self._normalize_table_ident(m_output.group(2))
        exprs = re.split(r',(?![^\(]*\))', out_exprs)
        cols = []
        for e in exprs:
            e = e.strip()
            ma = re.search(r'(?is)\bAS\s+([\w\[\]]+)$', e)
            if ma:
                cols.append(ma.group(1).strip('[]'))
            else:
                ms = re.search(r'(?is)([\w\[\]]+)$', e)
                if ms:
                    cols.append(ms.group(1).strip('[]'))
        seen = set()
        out = []
        for c in cols:
            lc = c.lower()
            if lc in seen:
                continue
            seen.add(lc)
            out.append(c)
        cols = out
        output_columns = [ColumnSchema(name=c, data_type=None, nullable=True, ordinal=i) for i, c in enumerate(cols)]
        tp = out_target.split('.')
        if len(tp) >= 3:
            target_table = f"{tp[-2]}.{tp[-1]}"
        elif len(tp) == 2:
            target_table = out_target
        else:
            target_table = f"dbo.{out_target}"

    # Dependencies (expand temp sources to underlying base tables if known)
    if source_name:
        simple_src = source_name.split('.')[-1]
        if simple_src in self.temp_registry:
            # Add the temp itself for visibility
            dependencies.add(source_name)
            # Add any previously recorded base sources for this temp
            try:
                temp_key = simple_src if simple_src.startswith('#') else f"#{simple_src}"
                base_sources = self.temp_sources.get(temp_key, set()) or set()
                for b in base_sources:
                    # Avoid self-dependency on target
                    if not (target_table and b.endswith(f".{target_table.split('.')[-1]}")):
                        dependencies.add(b)
            except Exception:
                pass
            # Fallback: basic dependency extraction (may include additional tables)
            try:
                deps_basic = self._extract_basic_dependencies(cleaned)
                for d in deps_basic:
                    if not (target_table and d.endswith(f".{target_table.split('.')[-1]}")):
                        dependencies.add(d)
            except Exception:
                pass
        else:
            dependencies.add(source_name)

    # Edges
    if target_table:
        ns_src_default, nm_src_default = (None, None)
        if source_name:
            ns_src_default, nm_src_default = self._ns_and_name(source_name)
        for (t_col, expr) in assign_exprs:
            refs: List[ColumnReference] = []
            for m in re.finditer(r'(?i)\b([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)\b', expr):
                a, c = m.group(1).lower(), m.group(2)
                if src_alias and a == src_alias.lower():
                    if ns_src_default and nm_src_default:
                        refs.append(ColumnReference(namespace=ns_src_default, table_name=nm_src_default, column_name=c))
                    continue
                if tgt_alias and a == tgt_alias.lower():
                    continue
                try:
                    ns2, nm2 = self._ns_and_name(a)
                    refs.append(ColumnReference(namespace=ns2, table_name=nm2, column_name=c))
                except Exception:
                    if ns_src_default and nm_src_default:
                        refs.append(ColumnReference(namespace=ns_src_default, table_name=nm_src_default, column_name=c))
            if not refs and ns_src_default and nm_src_default:
                mlast = re.search(r'(?i)\b([A-Za-z_][\w]*)\b$', expr)
                if mlast:
                    refs.append(ColumnReference(namespace=ns_src_default, table_name=nm_src_default, column_name=mlast.group(1)))

            # If source is a temp table and we have recorded per-column lineage, inline its base refs
            try:
                if source_name and (source_name.startswith('#') or source_name.lower().startswith('tempdb..#')):
                    temp_simple = self._extract_temp_name(source_name)
                    if not temp_simple.startswith('#'):
                        temp_simple = f"#{temp_simple}"
                    # Prefer versioned key if exists
                    ver_key = self._temp_current(temp_simple) or temp_simple
                    col_map = self.temp_lineage.get(ver_key) or self.temp_lineage.get(temp_simple) or {}
                    base_refs = col_map.get(t_col)
                    if base_refs:
                        # Add base refs, avoiding duplicates
                        seen_keys = {(r.namespace, r.table_name, r.column_name) for r in refs}
                        for br in base_refs:
                            key = (getattr(br, 'namespace', None), getattr(br, 'table_name', None), getattr(br, 'column_name', None))
                            if key not in seen_keys:
                                refs.append(br)
                                seen_keys.add(key)
                        # Also include the temp itself explicitly if not already
                        # Use _ns_and_name to get proper format with procedure context: schema.object#temp
                        temp_ns, temp_table_name = self._ns_and_name(temp_simple, obj_type_hint="temp_table")
                        temp_key = (temp_ns, temp_table_name, t_col)
                        if temp_key not in seen_keys:
                            refs.append(ColumnReference(namespace=temp_ns, table_name=temp_table_name, column_name=t_col))
            except Exception:
                pass
            lineage.append(ColumnLineage(
                output_column=t_col,
                input_fields=refs,
                transformation_type=TransformationType.EXPRESSION,
                transformation_description=f"MERGE expr: {t_col} = {expr.strip()}"
            ))

    return lineage, output_columns, dependencies, target_table

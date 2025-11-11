"""
SQL parsing and lineage extraction using SQLGlot.
"""
from __future__ import annotations

import logging
import re
from typing import List, Optional, Set, Dict, Any

import sqlglot
from sqlglot import expressions as exp

from .models import (
    ColumnReference, ColumnSchema, TableSchema, ColumnLineage, 
    TransformationType, ObjectInfo, SchemaRegistry, ColumnNode
)

logger = logging.getLogger(__name__)

# Note: light-scan regexes moved or inlined where needed

# moved to parser_modules.names: _cached_split_fqn_core


class SqlParser:
    """Parser for SQL statements using SQLGlot."""
    
    def __init__(self, dialect: str = "tsql", registry=None):
        self.dialect = dialect
        self.schema_registry = SchemaRegistry()
        self.cte_registry: Dict[str, List[str]] = {}  # CTE name -> column list
        self.temp_registry: Dict[str, List[str]] = {}  # Temp table name -> column list
        # Track temp table sources (base deps) and per-column lineage
        self.temp_sources: Dict[str, Set[str]] = {}  # "#tmp" -> base deps (schema.table)
        self.temp_lineage: Dict[str, Dict[str, List[ColumnReference]]] = {}  # "#tmp" or "#tmp@v" -> col -> [refs]
        # Procedure-level accumulator for multiple INSERTs into the same target
        # target schema.table -> { out_col -> set of (ns, table, col) }
        self._proc_acc: Dict[str, Dict[str, Set[tuple[str, str, str]]]] = {}
        # Temp versioning within a file/procedure: "#tmp" -> current version number
        self._temp_version: Dict[str, int] = {}
        self.default_database: Optional[str] = None  # Will be set from config
        self.current_database: Optional[str] = None  # Track current database context
        # Current object context (db + schema.object) for canonical temp naming – parity with legacy dev_parser
        self._ctx_db: Optional[str] = None
        self._ctx_obj: Optional[str] = None
        # cross-file object→DB registry (optional)
        self.registry = registry
        # dbt specifics
        self.dbt_mode: bool = False
        self.default_schema: Optional[str] = "dbo"
        # Track current file name for logging context
        self._current_file: Optional[str] = None
    
    # ---- Helpers: procedure accumulator ----
    def _proc_acc_init(self, target_fqn: str) -> None:
        from .parser_modules import temp_utils as _tu
        return _tu._proc_acc_init(self, target_fqn)

    def _proc_acc_add(self, target_fqn: str, col_lineage: List[ColumnLineage]) -> None:
        from .parser_modules import temp_utils as _tu
        return _tu._proc_acc_add(self, target_fqn, col_lineage)

    def _proc_acc_finalize(self, target_fqn: str) -> List[ColumnLineage]:
        from .parser_modules import temp_utils as _tu
        return _tu._proc_acc_finalize(self, target_fqn)

    # ---- Helpers: temp versioning ----
    def _temp_next(self, name: str) -> str:
        from .parser_modules import temp_utils as _tu
        return _tu._temp_next(self, name)

    def _temp_current(self, name: str) -> Optional[str]:
        from .parser_modules import temp_utils as _tu
        return _tu._temp_current(self, name)

    def _canonical_temp_name(self, name: str) -> str:
        from .parser_modules import temp_utils as _tu
        return _tu._canonical_temp_name(self, name)

    def _extract_temp_name(self, raw_name: str) -> str:
        from .parser_modules import temp_utils as _tu
        return _tu._extract_temp_name(self, raw_name)
    
    def _clean_proc_name(self, s: str) -> str:
        """Clean procedure name by removing semicolons and parameters."""
        from .parser_modules import names as _names
        return _names._clean_proc_name(self, s)
    
    def _normalize_table_ident(self, s: str) -> str:
        """Remove brackets and normalize table identifier."""
        from .parser_modules import names as _names
        return _names._normalize_table_ident(self, s)
    
    def _normalize_tsql(self, text: str) -> str:
        """Normalize T-SQL to improve sqlglot parsing compatibility."""
        from .parser_modules import preprocess as _pp
        return _pp._normalize_tsql(self, text)
    
    def _rewrite_ast(self, root: Optional[exp.Expression]) -> Optional[exp.Expression]:
        from .parser_modules import preprocess as _pp
        return _pp._rewrite_ast(self, root)

    # ---- Logging helpers with file context ----
    def _log_info(self, msg: str, *args) -> None:
        prefix = f"[file={self._current_file or '-'}] "
        try:
            text = (msg % args) if args else str(msg)
        except Exception:
            text = str(msg)
        # strip ANSI/BiDi from log text for readability
        try:
            text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)
            text = re.sub(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]", "", text)
        except Exception:
            pass
        logger.info(prefix + text)

    def _log_warning(self, msg: str, *args) -> None:
        prefix = f"[file={self._current_file or '-'}] "
        try:
            text = (msg % args) if args else str(msg)
        except Exception:
            text = str(msg)
        try:
            text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)
            text = re.sub(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]", "", text)
        except Exception:
            pass
        logger.warning(prefix + text)

    def _log_debug(self, msg: str, *args) -> None:
        prefix = f"[file={self._current_file or '-'}] "
        try:
            text = (msg % args) if args else str(msg)
        except Exception:
            text = str(msg)
        try:
            text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)
            text = re.sub(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]", "", text)
        except Exception:
            pass
        logger.debug(prefix + text)

    def _extract_dbt_model_name(self, sql_text: str) -> Optional[str]:
        from .parser_modules import preprocess as _pp
        return _pp._extract_dbt_model_name(self, sql_text)
    
    def _split_fqn(self, fqn: str):
        """Split fully qualified name into database, schema, table components (uses cached core)."""
        from .parser_modules import names as _names
        return _names._split_fqn(self, fqn)

    def _ns_and_name(self, table_name: str, obj_type_hint: str = "table") -> tuple[str, str]:
        from .parser_modules import names as _names
        return _names._ns_and_name(self, table_name, obj_type_hint)

    def _canonical_namespace(self, db: str | None) -> str:
        from .parser_modules import names as _names
        return _names._canonical_namespace(self, db)

    # -- DB inference helpers (delegated)
    def _strip_sql_comments(self, sql: str) -> str:
        from .parser_modules import db_infer as _db
        return _db._strip_sql_comments(self, sql)

    def _infer_db_candidates_from_ast(self, node):
        from .parser_modules import db_infer as _db
        return _db._infer_db_candidates_from_ast(self, node)

    def _infer_db_candidates_from_sql(self, sql_text: str):
        from .parser_modules import db_infer as _db
        return _db._infer_db_candidates_from_sql(self, sql_text)

    def _choose_db(self, counter) -> Optional[str]:
        from .parser_modules import db_infer as _db
        return _db._choose_db(self, counter)

    def _infer_database_for_object(self, statement=None, sql_text: Optional[str] = None) -> Optional[str]:
        """Infer DB for an object created without explicit DB in its name."""
        from .parser_modules import db_infer as _db
        return _db._infer_database_for_object(self, statement, sql_text)
    
    def _qualify_table(self, tbl: exp.Table) -> str:
        """Get fully qualified table name from Table expression."""
        from .parser_modules import names as _names
        return _names._qualify_table(self, tbl)
    
    def _build_alias_maps(self, select_exp: exp.Select):
        from .parser_modules import select_lineage as _sl
        return _sl._build_alias_maps(self, select_exp)
    
    def _append_column_ref(self, out_list, col_exp: exp.Column, alias_map: dict):
        from .parser_modules import select_lineage as _sl
        return _sl._append_column_ref(self, out_list, col_exp, alias_map)
    
    def _collect_inputs_for_expr(self, expr: exp.Expression, alias_map: dict, derived_cols: dict):
        from .parser_modules import select_lineage as _sl
        return _sl._collect_inputs_for_expr(self, expr, alias_map, derived_cols)
    
    def _get_schema(self, db: str, sch: str, tbl: str):
        from .parser_modules import select_lineage as _sl
        return _sl._get_schema(self, db, sch, tbl)
    
    def _type_of_column(self, col_exp, alias_map):
        from .parser_modules import select_lineage as _sl
        return _sl._type_of_column(self, col_exp, alias_map)
    
    def _infer_type(self, expr, alias_map) -> str:
        from .parser_modules import select_lineage as _sl
        return _sl._infer_type(self, expr, alias_map)
    
    def _short_desc(self, expr) -> str:
        from .parser_modules import select_lineage as _sl
        return _sl._short_desc(self, expr)
    
    def _extract_view_header_cols(self, create_exp) -> list[str]:
        from .parser_modules import select_lineage as _sl
        return _sl._extract_view_header_cols(self, create_exp)
    
    def set_default_database(self, default_database: Optional[str]):
        """Set the default database for qualification."""
        self.default_database = default_database
    
    def set_default_schema(self, default_schema: Optional[str]):
        """Set the default schema for dbt-mode normalization."""
        if default_schema:
            self.default_schema = default_schema
        return self

    def enable_dbt_mode(self, enabled: bool = True):
        """Enable/disable dbt mode (compiled SELECT-only models)."""
        self.dbt_mode = bool(enabled)
        return self
    
    def _extract_database_from_use_statement(self, content: str) -> Optional[str]:
        """Extract database name from USE statement at the beginning of file."""
        from .parser_modules import preprocess as _pp
        return _pp._extract_database_from_use_statement(self, content)
    
    def _get_full_table_name(self, table_name: str) -> str:
        from .parser_modules import names as _names
        return _names._get_full_table_name(self, table_name)
    
    def _preprocess_sql(self, sql: str) -> str:
        """
        Preprocess SQL to remove control lines and join INSERT INTO #temp EXEC patterns.
        Also extracts database context from USE statements.
        """
        from .parser_modules import preprocess as _pp
        return _pp._preprocess_sql(self, sql)
    
    def _cut_to_first_statement(self, sql: str) -> str:
        """
        Cut SQL content to start from the first significant statement.
        Looks for: CREATE [OR ALTER] VIEW|TABLE|FUNCTION|PROCEDURE, ALTER, SELECT...INTO, INSERT...EXEC
        """
        from .parser_modules import preprocess as _pp
        return _pp._cut_to_first_statement(self, sql)
    
    def _try_insert_exec_fallback(self, sql_content: str, object_hint: Optional[str] = None) -> Optional[ObjectInfo]:
        from .parser_modules import string_fallbacks as _sf
        return _sf._try_insert_exec_fallback(self, sql_content, object_hint)
    
    def _find_last_select_string(self, sql_content: str, dialect: str = "tsql") -> str | None:
        from .parser_modules import string_fallbacks as _sf
        return _sf._find_last_select_string(self, sql_content, dialect)

    def _find_last_select_string_fallback(self, sql_content: str) -> str | None:
        from .parser_modules import string_fallbacks as _sf
        return _sf._find_last_select_string_fallback(self, sql_content)
    
    def parse_sql_file(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse a SQL file and extract object information."""
        from .openlineage_utils import sanitize_name
        
        # Track current file for log context. If engine pre-set _current_file (real file path), keep it.
        prev_file = self._current_file
        if not self._current_file:
            self._current_file = object_hint or prev_file
        # Reset current database to default for each file
        self.current_database = self.default_database
        # Keep the raw SQL for DB inference when object name lacks explicit DB
        self._current_raw_sql = sql_content
        # Diagnostics: log RETURNS...AS window (escaped) after lightweight normalization
        try:
            dbg = self._normalize_tsql(sql_content)
            m = re.search(r'(?is)\bRETURNS\b(.{0,120}?)\bAS\b', dbg)
            window = m.group(0) if m else "<no match>"
            self._log_debug("RETURNS-window=%r", window)
        except Exception:
            pass
        
        # Reset registries for each file to avoid contamination
        self.cte_registry.clear()
        self.temp_registry.clear()
        self.temp_sources.clear()
        self.temp_lineage.clear()
        self._proc_acc.clear()
        self._temp_version.clear()
        
        try:
            # dbt mode: compiled SELECT-only models; derive target name from filename
            if self.dbt_mode:
                normalized_sql = self._normalize_tsql(sql_content)
                preprocessed_sql = self._preprocess_sql(normalized_sql)
                # Ephemeral-like dbt model: no final SELECT, but has INSERT INTO #temp
                import re as _re
                if _re.search(r"(?is)\bINSERT\s+INTO\s+#", preprocessed_sql):
                    model_name = self._extract_dbt_model_name(sql_content) or sanitize_name(object_hint or "dbt_model")
                    nm = f"{self.default_schema or 'dbo'}.{model_name}"
                    db = self.current_database or self.default_database or "InfoTrackerDW"
                    db_ns = str(db).upper()
                    deps = self._extract_basic_dependencies(preprocessed_sql)
                    deps_norm: Set[str] = set()
                    for dep in deps:
                        dep_s = sanitize_name(dep)
                        parts = dep_s.split('.') if dep_s else []
                        tbl = parts[-1] if parts else dep_s
                        deps_norm.add(f"{self.default_schema or 'dbo'}.{tbl}")
                    schema = TableSchema(namespace=self._canonical_namespace(db_ns), name=nm, columns=[])
                    self.schema_registry.register(schema)
                    obj = ObjectInfo(name=nm, object_type="view", schema=schema, lineage=[], dependencies=deps_norm)
                    obj.is_fallback = True
                    obj.no_output_reason = "DBT_NO_FINAL_SELECT"
                    try:
                        if object_hint:
                            obj.job_name = f"dbt/models/{object_hint}.sql"
                    except Exception:
                        pass
                    return obj
                statements = sqlglot.parse(preprocessed_sql, read=self.dialect) or []
                # Consider only a top-level final SELECT as a compiled dbt model output
                last_select = None
                top_stmt = statements[-1] if statements else None
                if isinstance(top_stmt, exp.Select):
                    last_select = top_stmt
                if last_select is not None:
                    # Prefer model name from header comment; fallback to filename stem
                    model_name = self._extract_dbt_model_name(sql_content) or sanitize_name(object_hint or "dbt_model")
                    nm = f"{self.default_schema or 'dbo'}.{model_name}"
                    db = self.current_database or self.default_database or "InfoTrackerDW"
                    # For dbt ephemeral fallback, tests expect uppercase DB in namespace
                    db_ns = str(db).upper()
                    # Dependencies and lineage from last SELECT
                    deps = self._extract_dependencies(last_select)
                    deps_norm: Set[str] = set()
                    for dep in deps:
                        dep_s = sanitize_name(dep)
                        parts = dep_s.split('.') if dep_s else []
                        tbl = parts[-1] if parts else dep_s
                        deps_norm.add(f"{self.default_schema or 'dbo'}.{tbl}")
                    lineage, output_columns = self._extract_column_lineage(last_select, nm)
                    # Decide object type: schema-only SELECT (no FROM) -> treat as table (seed/source)
                    has_from_tables = any(True for _ in last_select.find_all(exp.Table))
                    obj_type = "view" if has_from_tables else "table"
                    schema = TableSchema(
                        namespace=self._canonical_namespace(db),
                        name=nm,
                        columns=output_columns
                    )
                    self.schema_registry.register(schema)
                    obj = ObjectInfo(
                        name=nm,
                        object_type=obj_type,
                        schema=schema,
                        lineage=lineage,
                        dependencies=deps_norm
                    )
                    try:
                        # Set dbt-style job path for OL emitter
                        if object_hint:
                            obj.job_name = f"dbt/models/{object_hint}.sql"
                    except Exception:
                        pass
                    return obj
                else:
                    # Fallback for dbt files that don't expose a final SELECT (non-materializing/ephemeral patterns)
                    model_name = self._extract_dbt_model_name(sql_content) or sanitize_name(object_hint or "dbt_model")
                    nm = f"{self.default_schema or 'dbo'}.{model_name}"
                    db = self.current_database or self.default_database or "InfoTrackerDW"
                    deps = self._extract_basic_dependencies(preprocessed_sql)
                    # Normalize deps to default schema.table
                    deps_norm: Set[str] = set()
                    for dep in deps:
                        dep_s = sanitize_name(dep)
                        parts = dep_s.split('.') if dep_s else []
                        tbl = parts[-1] if parts else dep_s
                        deps_norm.add(f"{self.default_schema or 'dbo'}.{tbl}")
                    schema = TableSchema(
                        namespace=self._canonical_namespace(db_ns),
                        name=nm,
                        columns=[]
                    )
                    self.schema_registry.register(schema)
                    obj = ObjectInfo(
                        name=nm,
                        object_type="view",
                        schema=schema,
                        lineage=[],
                        dependencies=deps_norm
                    )
                    obj.is_fallback = True
                    obj.no_output_reason = "DBT_NO_FINAL_SELECT"
                    try:
                        if object_hint:
                            obj.job_name = f"dbt/models/{object_hint}.sql"
                    except Exception:
                        pass
                    return obj
            # Check if this file contains multiple objects and handle accordingly
            sql_upper = sql_content.upper()
            
            # Count how many CREATE statements we have (robust to PROC/PROCEDURE)
            import re as _re
            def _count(pats: List[str]) -> int:
                return sum(len(_re.findall(p, sql_upper, flags=_re.I)) for p in pats)
            create_function_count = _count([r"\bCREATE\s+(?:OR\s+ALTER\s+)?FUNCTION\b"])
            create_procedure_count = _count([r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROCEDURE\b", r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROC\b"])
            create_table_count = _count([r"\bCREATE\s+(?:OR\s+ALTER\s+)?TABLE\b"])

            if create_table_count == 1 and all(x == 0 for x in [create_function_count, create_procedure_count]):
                # spróbuj najpierw AST; jeśli SQLGlot zwróci Command albo None — fallback stringowy
                try:
                    normalized_sql = self._normalize_tsql(sql_content)
                    statements = sqlglot.parse(self._preprocess_sql(normalized_sql), read=self.dialect) or []
                    st = statements[0] if statements else None
                    if st and isinstance(st, exp.Create) and (getattr(st, "kind", "") or "").upper() == "TABLE":
                        return self._parse_create_table(st, object_hint)
                except Exception:
                    pass
                return self._parse_create_table_string(sql_content, object_hint)
            # If it's a single function, use string-based approach (TVF-specific heuristics)
            if create_function_count == 1 and create_procedure_count == 0:
                return self._parse_function_string(sql_content, object_hint)
            # For a single procedure prefer AST parsing to preserve temp lineage; fallback to string only if AST fails
            elif create_procedure_count == 1 and create_function_count == 0:
                try:
                    normalized_sql = self._normalize_tsql(sql_content)
                    preprocessed_sql = self._preprocess_sql(normalized_sql)
                    stmts = sqlglot.parse(preprocessed_sql, read=self.dialect) or []
                    st = stmts[0] if stmts else None
                    if st and isinstance(st, exp.Create) and (getattr(st, "kind", "") or "").upper() == "PROCEDURE":
                        return self._parse_create_procedure(st, object_hint)
                except Exception:
                    pass
                return self._parse_procedure_string(sql_content, object_hint)
            
            # If it's multiple functions but no procedures, process the first function as primary
            # This handles files like 94_fn_customer_orders_tvf.sql with multiple function variants
            elif create_function_count > 1 and create_procedure_count == 0:
                # Extract and process the first function only for detailed lineage
                first_function_sql = self._extract_first_create_statement(sql_content, 'FUNCTION')
                if first_function_sql:
                    return self._parse_function_string(first_function_sql, object_hint)
            
            # If multiple objects or mixed content, use multi-statement processing
            # This handles demo scripts with multiple functions/procedures/statements
            
            # Preprocess the SQL content to handle demo script patterns
            # This will also extract and set current_database from USE statements
            normalized_sql = self._normalize_tsql(sql_content)
            preprocessed_sql = self._preprocess_sql(normalized_sql)
            
            # For files with complex IF/ELSE blocks, also try string-based extraction
            # This is needed for demo scripts like 96_demo_usage_tvf_and_proc.sql
            string_deps = set()
            # Parse all SQL statements with SQLGlot
            statements = sqlglot.parse(preprocessed_sql, read=self.dialect)
            
            # Apply AST rewrites to improve parsing (guard None)
            if statements:
                statements = [s for s in (self._rewrite_ast(s) for s in statements) if s]
            if not statements:
                # If SQLGlot parsing fails completely, try to extract dependencies with string parsing
                dependencies = self._extract_basic_dependencies(preprocessed_sql)
                # Provide a valid TableSchema instead of a raw list to avoid downstream crashes
                db = self.current_database or self.default_database or "InfoTrackerDW"
                nm = (object_hint or self._get_fallback_name(sql_content))
                return ObjectInfo(
                    name=nm,
                    object_type="script",
                    schema=TableSchema(
                        namespace=self._canonical_namespace(db),
                        name=nm,
                        columns=[],
                    ),
                    dependencies=dependencies,
                    lineage=[],
                )
            
            # Process the entire script - aggregate across all statements
            all_inputs = set()
            all_outputs = []
            main_object = None
            last_persistent_output = None
            
            # Process all statements in order
            for statement in statements:
                if isinstance(statement, exp.Create):
                    # This is the main object being created
                    obj = self._parse_create_statement(statement, object_hint)
                    if obj.object_type in ["table", "view", "function", "procedure"]:
                        last_persistent_output = obj
                    # Add inputs from DDL statements
                    all_inputs.update(obj.dependencies)
                    
                elif isinstance(statement, exp.Select) and self._is_select_into(statement):
                    # SELECT ... INTO creates a table/temp table
                    obj = self._parse_select_into(statement, object_hint)
                    all_outputs.append(obj)
                    # Check if it's persistent (not temp)
                    if not obj.name.startswith("#") and "tempdb" not in obj.name:
                        last_persistent_output = obj
                    all_inputs.update(obj.dependencies)
                    
                elif isinstance(statement, exp.Select):
                    # Loose SELECT statement - extract dependencies but no output
                    self._process_ctes(statement)
                    stmt_deps = self._extract_dependencies(statement)
                    
                    # Expand CTEs and temp tables to base tables
                    for dep in stmt_deps:
                        expanded_deps = self._expand_dependency_to_base_tables(dep, statement)
                        all_inputs.update(expanded_deps)
                    
                elif isinstance(statement, exp.Insert):
                    if self._is_insert_exec(statement):
                        # INSERT INTO ... EXEC
                        obj = self._parse_insert_exec(statement, object_hint)
                        all_outputs.append(obj)
                        if not obj.name.startswith("#") and "tempdb" not in obj.name:
                            last_persistent_output = obj
                        all_inputs.update(obj.dependencies)
                    else:
                        # INSERT INTO ... SELECT - this handles persistent tables
                        obj = self._parse_insert_select(statement, object_hint)
                        if obj:
                            all_outputs.append(obj)
                            # Accumulate per-target lineage across branches for procedures/scripts
                            try:
                                self._proc_acc_init(obj.name)
                                self._proc_acc_add(obj.name, obj.lineage or [])
                            except Exception:
                                pass
                            # Check if this is a persistent table (main output)
                            if not obj.name.startswith("#") and "tempdb" not in obj.name.lower():
                                last_persistent_output = obj
                            all_inputs.update(obj.dependencies)
                
                # Extra: guard for INSERT variants parsed oddly by SQLGlot (Command inside expression)
                elif hasattr(statement, "this") and isinstance(statement, exp.Table) and "INSERT" in str(statement).upper():
                    # Best-effort: try _parse_insert_select fallback if AST is quirky
                    try:
                        obj = self._parse_insert_select(statement, object_hint)
                        if obj:
                            all_outputs.append(obj)
                            if not obj.name.startswith("#") and "tempdb" not in obj.name.lower():
                                last_persistent_output = obj
                            all_inputs.update(obj.dependencies)
                    except Exception:
                        pass
                        
                elif isinstance(statement, exp.With):
                    # Process WITH statements (CTEs)
                    if hasattr(statement, 'this') and isinstance(statement.this, exp.Select):
                        self._process_ctes(statement.this)
                        stmt_deps = self._extract_dependencies(statement.this)
                        for dep in stmt_deps:
                            expanded_deps = self._expand_dependency_to_base_tables(dep, statement.this)
                            all_inputs.update(expanded_deps)
            
            # Remove CTE references from final inputs
            all_inputs = {dep for dep in all_inputs if not self._is_cte_reference(dep)}
            
            # Sanitize all input names
            all_inputs = {sanitize_name(dep) for dep in all_inputs if dep}
            def _strip_db(name: str) -> str:
                parts = (name or "").split(".")
                return ".".join(parts[-2:]) if len(parts) >= 2 else (name or "")

            # Only compute out_key if we have a persistent output
            out_key = None
            if last_persistent_output is not None:
                out_key = _strip_db(sanitize_name(
                    (last_persistent_output.schema.name if getattr(last_persistent_output, 'schema', None) else last_persistent_output.name)
                ))
            if out_key:
                all_inputs = {d for d in all_inputs if _strip_db(sanitize_name(d)) != out_key}
                # Determine the main object
            if last_persistent_output:
                # Use the last persistent output as the main object
                main_object = last_persistent_output
                # Update its dependencies with all collected inputs
                main_object.dependencies = all_inputs
                # If we accumulated lineage across multiple branches for this target, finalize it
                try:
                    merged = self._proc_acc_finalize(main_object.name)
                    if merged:
                        main_object.lineage = merged
                except Exception:
                    pass
            elif all_outputs:
                # Use the last output if no persistent one found
                main_object = all_outputs[-1]
                main_object.dependencies = all_inputs
            elif all_inputs:
                # Create a file-level object with aggregated inputs (for demo scripts)
                db = self.current_database or self.default_database or "InfoTrackerDW"
                main_object = ObjectInfo(
                    name=sanitize_name(object_hint or "loose_statements"),
                    object_type="script",
                    schema=TableSchema(
                        namespace=self._canonical_namespace(db),
                        name=sanitize_name(object_hint or "loose_statements"),
                        columns=[]
                    ),
                    lineage=[],
                    dependencies=all_inputs
                )
                # Add no-output reason for diagnostics
                if not self.current_database and not self.default_database:
                    main_object.no_output_reason = "UNKNOWN_DB_CONTEXT"
                else:
                    main_object.no_output_reason = "NO_PERSISTENT_OUTPUT_DETECTED"
            
            if main_object:
                return main_object
            else:
                raise ValueError("No valid statements found to process")
                
        except Exception as e:
            # Try fallback for INSERT INTO #temp EXEC pattern
            fallback_result = self._try_insert_exec_fallback(sql_content, object_hint)
            if fallback_result:
                return fallback_result
            
            # Include object hint to help identify the failing file
            try:
                self._log_warning("parse failed (object=%s): %s", object_hint, e)
            except Exception:
                self._log_warning("parse failed: %s", e)
            # Return an object with error information (dbt-aware fallback)
            db = self.current_database or self.default_database or "InfoTrackerDW"
            model_name = sanitize_name(object_hint or "unknown")
            nm = f"{self.default_schema or 'dbo'}.{model_name}" if getattr(self, 'dbt_mode', False) else model_name
            obj = ObjectInfo(
                name=nm,
                object_type="unknown",
                schema=TableSchema(
                    namespace=self._canonical_namespace(db),
                    name=nm,
                    columns=[]
                ),
                lineage=[],
                dependencies=set()
            )
            # Ensure dbt-style job path if applicable
            try:
                if getattr(self, 'dbt_mode', False) and object_hint:
                    obj.job_name = f"dbt/models/{object_hint}.sql"
            except Exception:
                pass
            # Restore previous file context before returning
            self._current_file = prev_file
            return obj
    
    def _is_select_into(self, statement: exp.Select) -> bool:
        """Check if this is a SELECT INTO statement."""
        from .parser_modules import dml as _dml
        return _dml._is_select_into(self, statement)
    
    def _is_insert_exec(self, statement: exp.Insert) -> bool:
        """Check if this is an INSERT INTO ... EXEC statement."""
        from .parser_modules import dml as _dml
        return _dml._is_insert_exec(self, statement)
    
    def _parse_select_into(self, statement: exp.Select, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse SELECT INTO statement."""
        from .parser_modules import dml as _dml
        return _dml._parse_select_into(self, statement, object_hint)
    
    def _parse_insert_exec(self, statement: exp.Insert, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse INSERT INTO ... EXEC statement."""
        from .parser_modules import dml as _dml
        return _dml._parse_insert_exec(self, statement, object_hint)
    
    def _parse_insert_select(self, statement: exp.Insert, object_hint: Optional[str] = None) -> Optional[ObjectInfo]:
        """Parse INSERT INTO ... SELECT statement."""
        from .parser_modules import dml as _dml
        return _dml._parse_insert_select(self, statement, object_hint)
    
    def _parse_create_statement(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse CREATE TABLE, CREATE VIEW, CREATE FUNCTION, or CREATE PROCEDURE statement."""
        if statement.kind == "TABLE":
            return self._parse_create_table(statement, object_hint)
        elif statement.kind == "VIEW":
            return self._parse_create_view(statement, object_hint)
        elif statement.kind == "FUNCTION":
            return self._parse_create_function(statement, object_hint)
        elif statement.kind == "PROCEDURE":
            return self._parse_create_procedure(statement, object_hint)
        else:
            raise ValueError(f"Unsupported CREATE statement: {statement.kind}")
    
    def _parse_create_table(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        from .parser_modules import create_handlers as _ch
        return _ch._parse_create_table(self, statement, object_hint)
    
    def _parse_create_table_string(self, sql: str, object_hint: Optional[str] = None) -> ObjectInfo:
        from .parser_modules import create_handlers as _ch
        return _ch._parse_create_table_string(self, sql, object_hint)

    def _parse_create_view(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        from .parser_modules import create_handlers as _ch
        return _ch._parse_create_view(self, statement, object_hint)
    
    def _parse_create_function(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        from .parser_modules import create_handlers as _ch
        return _ch._parse_create_function(self, statement, object_hint)
    
    def _parse_create_procedure(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        from .parser_modules import create_handlers as _ch
        return _ch._parse_create_procedure(self, statement, object_hint)

    def _extract_procedure_outputs(self, statement: exp.Create) -> List[ObjectInfo]:
        from .parser_modules import create_handlers as _ch
        return _ch._extract_procedure_outputs(self, statement)

    def _extract_merge_lineage_string(self, sql_content: str, procedure_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str], Optional[str]]:
        """Delegate to string fallback MERGE lineage extractor."""
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_merge_lineage_string(self, sql_content, procedure_name)
    
    def _normalize_table_name_for_output(self, table_name: str) -> str:
        from .parser_modules import names as _names
        return _names._normalize_table_name_for_output(self, table_name)
    
    def _get_table_name(self, table_expr: exp.Expression, hint: Optional[str] = None) -> str:
        from .parser_modules import names as _names
        return _names._get_table_name(self, table_expr, hint)
    
    # column type/constraint helpers moved to create_handlers
    
    def _extract_dependencies(self, stmt: exp.Expression) -> Set[str]:
        from .parser_modules import deps as _deps
        return _deps._extract_dependencies(self, stmt)
    
    def _extract_column_lineage(self, stmt: exp.Expression, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
        from .parser_modules import select_lineage as _sl
        return _sl._extract_column_lineage(self, stmt, view_name)
    
    def _analyze_expression_lineage(self, output_name: str, expr: exp.Expression, context: exp.Select) -> ColumnLineage:
        from .parser_modules import select_lineage as _sl
        return _sl._analyze_expression_lineage(self, output_name, expr, context)
    
    def _resolve_table_from_alias(self, alias: Optional[str], context: exp.Select) -> str:
        from .parser_modules import select_lineage as _sl
        return _sl._resolve_table_from_alias(self, alias, context)
    
    def _process_ctes(self, select_stmt: exp.Select) -> exp.Select:
        from .parser_modules import select_lineage as _sl
        return _sl._process_ctes(self, select_stmt)
    
    def _is_string_function(self, expr: exp.Expression) -> bool:
        from .parser_modules import select_lineage as _sl
        return _sl._is_string_function(self, expr)
    
    def _has_star_expansion(self, select_stmt: exp.Select) -> bool:
        from .parser_modules import select_lineage as _sl
        return _sl._has_star_expansion(self, select_stmt)
    
    def _has_union(self, stmt: exp.Expression) -> bool:
        from .parser_modules import select_lineage as _sl
        return _sl._has_union(self, stmt)
    
    def _handle_star_expansion(self, select_stmt: exp.Select, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
        from .parser_modules import select_lineage as _sl
        return _sl._handle_star_expansion(self, select_stmt, view_name)

    
    def _handle_union_lineage(self, stmt: exp.Expression, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
        from .parser_modules import select_lineage as _sl
        return _sl._handle_union_lineage(self, stmt, view_name)
    
    def _infer_table_columns(self, table_name: str) -> List[str]:
        """Infer table columns using registry-based approach."""
        return self._infer_table_columns_unified(table_name)
    
    def _infer_table_columns_unified(self, table_name: str) -> List[str]:
        """Unified column lookup using registry chain: temp -> cte -> schema -> fallback."""
        # Clean table name for registry lookup
        simple_name = table_name.split('.')[-1]
        
        # 1. Check temp_registry first
        if simple_name in self.temp_registry:
            return self.temp_registry[simple_name]
        
        # 2. Check cte_registry
        if simple_name in self.cte_registry:
            return self.cte_registry[simple_name]
        
        # 3. Check schema_registry
        namespace = self._get_namespace_for_table(table_name)
        table_schema = self.schema_registry.get(namespace, table_name)
        if table_schema and table_schema.columns:
            return [col.name for col in table_schema.columns]
        
        # 4. Fallback to deterministic unknown columns (no hardcoding)
        return [f"unknown_{i+1}" for i in range(3)]  # Generate unknown_1, unknown_2, unknown_3
    
    def _get_namespace_for_table(self, table_name: str) -> str:
        from .parser_modules import names as _names
        return _names._get_namespace_for_table(self, table_name)

    def _parse_function_string(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse CREATE FUNCTION using string-based approach (delegated)."""
        from .parser_modules import functions as _func
        return _func._parse_function_string(self, sql_content, object_hint)
    
    def _parse_procedure_string(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse CREATE PROCEDURE using string-based approach (delegated)."""
        from .parser_modules import procedures as _proc
        return _proc._parse_procedure_string(self, sql_content, object_hint)

    def _extract_insert_select_lineage_string(self, sql_content: str, object_name: str) -> tuple[List[ColumnLineage], Set[str]]:
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_insert_select_lineage_string(self, sql_content, object_name)


    def _extract_materialized_output_from_procedure_string(self, sql_content: str) -> Optional[ObjectInfo]:
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_materialized_output_from_procedure_string(self, sql_content)

    def _extract_update_from_lineage_string(self, sql_content: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str], Optional[str]]:
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_update_from_lineage_string(self, sql_content)

    def _extract_output_into_lineage_string(self, sql_content: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str], Optional[str]]:
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_output_into_lineage_string(self, sql_content)
    
    def _extract_function_name(self, sql_content: str) -> Optional[str]:
        """Extract function name from CREATE FUNCTION statement (delegated)."""
        from .parser_modules import functions as _func
        return _func._extract_function_name(self, sql_content)
    
    def _extract_procedure_name(self, sql_content: str) -> Optional[str]:
        """Extract procedure name from CREATE PROCEDURE statement (delegated)."""
        from .parser_modules import procedures as _proc
        return _proc._extract_procedure_name(self, sql_content)

    def _is_table_valued_function_string(self, sql_content: str) -> bool:
        """Check if this is a table-valued function (delegated)."""
        from .parser_modules import functions as _func
        return _func._is_table_valued_function_string(self, sql_content)
    
    
    
    def _extract_procedure_lineage_string(self, sql_content: str, procedure_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_procedure_lineage_string(self, sql_content, procedure_name)
    
    def _extract_insert_into_columns(self, sql_content: str) -> list[str]:
        m = re.search(r'(?is)INSERT\s+INTO\s+[^\s(]+\s*\((.*?)\)', sql_content)
        if not m:
            return []
        inner = m.group(1)
        cols = []
        for raw in inner.split(','):
            col = raw.strip()
            # zbij aliasy i nawiasy, zostaw samą nazwę
            col = col.split('.')[-1]
            col = re.sub(r'[^\w]', '', col)
            if col:
                cols.append(col)
        return cols



    def _extract_first_create_statement(self, sql_content: str, statement_type: str) -> str:
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_first_create_statement(self, sql_content, statement_type)

    def _extract_tvf_lineage_string(self, sql_text: str, function_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_tvf_lineage_string(self, sql_text, function_name)

    def _extract_select_from_return_string(self, sql_content: str) -> Optional[str]:
        """Extract SELECT statement from RETURN clause (delegated)."""
        from .parser_modules import functions as _func
        return _func._extract_select_from_return_string(self, sql_content)
    
    def _extract_table_variable_schema_string(self, sql_content: str) -> List[ColumnSchema]:
        """Extract column schema from @table TABLE definition using regex."""
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_table_variable_schema_string(self, sql_content)
        

    
    def _extract_basic_select_columns(self, select_sql: str) -> List[ColumnSchema]:
        """Basic extraction of column names from SELECT statement."""
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_basic_select_columns(self, select_sql)

    def _extract_basic_lineage_from_select(self, select_sql: str, output_columns: List[ColumnSchema], object_name: str) -> List[ColumnLineage]:
        """Extract basic lineage information from SELECT statement using string parsing."""
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_basic_lineage_from_select(self, select_sql, output_columns, object_name)
    
    def _extract_table_aliases_from_select(self, select_sql: str) -> Dict[str, str]:
        """Extract table aliases from FROM and JOIN clauses."""
        from .parser_modules import string_fallbacks as _sf
        return _sf._extract_table_aliases_from_select(self, select_sql)
    
    def _parse_column_expression(self, col_expr: str, table_aliases: Dict[str, str]) -> tuple[str, str, TransformationType]:
        """Parse a column expression to find source table, column, and transformation type."""
        from .parser_modules import string_fallbacks as _sf
        return _sf._parse_column_expression(self, col_expr, table_aliases)

    def _extract_basic_dependencies(self, sql_content: str) -> Set[str]:
        """Basic extraction of table dependencies from SQL (delegated)."""
        from .parser_modules import deps as _deps
        return _deps._extract_basic_dependencies(self, sql_content)
        

    def _is_table_valued_function(self, statement: exp.Create) -> bool:
        """Check if this is a table-valued function (delegated)."""
        from .parser_modules import create_handlers as _ch
        return _ch._is_table_valued_function(self, statement)
    
    def _extract_tvf_lineage(self, statement: exp.Create, function_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
        """Extract lineage from a table-valued function."""
        from .parser_modules import create_handlers as _ch
        return _ch._extract_tvf_lineage(self, statement, function_name)
    
    def _extract_procedure_lineage(self, statement: exp.Create, procedure_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema], Set[str]]:
        """Extract lineage from a procedure that returns a dataset."""
        from .parser_modules import create_handlers as _ch
        return _ch._extract_procedure_lineage(self, statement, procedure_name)
    
    def _extract_select_from_return(self, statement: exp.Create) -> Optional[exp.Select]:
        """Extract SELECT statement from RETURN AS clause."""
        from .parser_modules import create_handlers as _ch
        return _ch._extract_select_from_return(self, statement)
    
    def _extract_table_variable_schema(self, statement: exp.Create) -> List[ColumnSchema]:
        """Extract column schema from @table TABLE definition."""
        from .parser_modules import create_handlers as _ch
        return _ch._extract_table_variable_schema(self, statement)
    
    def _extract_mstvf_lineage(self, statement: exp.Create, function_name: str, output_columns: List[ColumnSchema]) -> tuple[List[ColumnLineage], Set[str]]:
        from .parser_modules import create_handlers as _ch
        return _ch._extract_mstvf_lineage(self, statement, function_name, output_columns)
    
    def _expand_dependency_to_base_tables(self, dep_name: str, context_stmt: exp.Expression) -> Set[str]:
        from .parser_modules import deps as _deps
        return _deps._expand_dependency_to_base_tables(self, dep_name, context_stmt)
    
    def _is_cte_reference(self, dep_name: str) -> bool:
        from .parser_modules import deps as _deps
        return _deps._is_cte_reference(self, dep_name)
    
    def _find_last_select_in_procedure(self, statement: exp.Create) -> Optional[exp.Select]:
        from .parser_modules import create_handlers as _ch
        return _ch._find_last_select_in_procedure(self, statement)
    
    def _extract_column_alias(self, select_expr: exp.Expression) -> Optional[str]:
        from .parser_modules import select_lineage as _sl
        return _sl._extract_column_alias(self, select_expr)
    
    def _extract_column_references(self, select_expr: exp.Expression, select_stmt: exp.Select) -> List[ColumnReference]:
        from .parser_modules import select_lineage as _sl
        return _sl._extract_column_references(self, select_expr, select_stmt)

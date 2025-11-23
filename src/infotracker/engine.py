# src/infotracker/engine.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from fnmatch import fnmatch

import yaml

from .adapters import get_adapter
from .object_db_registry import ObjectDbRegistry
from .io_utils import read_text_safely
from .lineage import emit_ol_from_object
from .models import (
    ObjectInfo, 
    ColumnNode, 
    ColumnSchema,
    TableSchema,
    ColumnGraph,
    ColumnEdge,
    ColumnLineage,
    ColumnReference,
    TransformationType,
)

logger = logging.getLogger(__name__)


# ======== Requests (sygnatury zgodne z CLI) ========

@dataclass
class ExtractRequest:
    sql_dir: Path
    out_dir: Path
    adapter: str
    catalog: Optional[Path] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    fail_on_warn: bool = False
    encoding: str = "auto"


@dataclass
class ImpactRequest:
    selector: str
    max_depth: int = 0
    graph_dir: Optional[Path] = None


@dataclass
class DiffRequest:
    base: str  # git ref for base
    head: str  # git ref for head
    sql_dir: Path
    adapter: str
    severity_threshold: str = "BREAKING"   # NON_BREAKING | POTENTIALLY_BREAKING | BREAKING


# ======== Engine ========

class Engine:
    def __init__(self, config: Any):
        """
        config: RuntimeConfig z cli/config.py
        Używamy:
        - config.include / config.exclude (opcjonalne listy)
        - config.ignore (opcjonalna lista wzorców obiektów do pominięcia)
        """
        self.config = config
        self._column_graph: Optional[ColumnGraph] = None
        # Emit minimal OL events for external inputs so they appear in viz
        try:
            self._emit_external_sources = bool(getattr(config, 'emit_external_sources', True))
        except Exception:
            self._emit_external_sources = True

    # ------------------ EXTRACT ------------------

    def run_extract(self, req: ExtractRequest) -> Dict[str, Any]:
        """
        1) (opcjonalnie) wczytaj catalog i zarejestruj tabele/kolumny w parser.schema_registry
        2) zbierz pliki wg include/exclude
        3) dla każdego pliku: parse -> adapter.extract_lineage (str lub dict) -> zapis JSON
        4) licz warnings na bazie outputs[0].facets (schema/columnLineage)
        5) zbuduj graf kolumn do późniejszego impact
        """
        adapter = get_adapter(req.adapter, self.config)
        # Apply dbt project context (default DB/schema) if in dbt mode
        try:
            if getattr(self.config, 'dbt_mode', False):
                self._apply_dbt_context(req.sql_dir, adapter)
        except Exception:
            pass
        parser = adapter.parser

        # Load global object→DB registry and inject into parser (shared across files)
        try:
            db_map_path = getattr(self.config, "object_db_map_path", "build/object_db_map.json")
        except Exception:
            db_map_path = "build/object_db_map.json"
        registry = ObjectDbRegistry.load(db_map_path)
        parser.registry = registry

        warnings = 0

        # 1) Catalog (opcjonalny)
        if req.catalog:
            catalog_path = Path(req.catalog)
            if catalog_path.exists():
                try:
                    catalog_data = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
                    tables = catalog_data.get("tables", [])
                    for t in tables:
                        namespace = t.get("namespace") or "mssql://localhost/InfoTrackerDW"
                        name = t["name"]
                        cols_raw = t.get("columns", [])
                        cols: List[ColumnSchema] = [
                            ColumnSchema(
                                name=c["name"],
                                data_type=c.get("type"),
                                nullable=bool(c.get("nullable", True)),
                                ordinal=int(c.get("ordinal", 0)),
                            )
                            for c in cols_raw
                        ]
                        parser.schema_registry.register(
                            TableSchema(namespace=namespace, name=name, columns=cols)
                        )
                except Exception as e:
                    warnings += 1
                    logger.warning("failed to load catalog from %s: %s", catalog_path, e)
            else:
                warnings += 1
                logger.warning("catalog path not found: %s", catalog_path)

        # 2) Include/Exclude (relative to sql_dir, robust to patterns like "**/file.sql" and "file.sql")
        includes: Optional[List[str]] = None
        excludes: Optional[List[str]] = None

        if getattr(req, "include", None):
            includes = list(req.include)
        elif getattr(self.config, "include", None):
            includes = list(self.config.include)

        if getattr(req, "exclude", None):
            excludes = list(req.exclude)
        elif getattr(self.config, "exclude", None):
            excludes = list(self.config.exclude)

        sql_root = Path(req.sql_dir)
        sql_files: List[Path] = []
        for p in sorted(sql_root.rglob("*.sql")):
            try:
                rel = p.relative_to(sql_root).as_posix()
            except Exception:
                rel = p.name
            inc_ok = True if not includes else any(
                fnmatch(rel, pat) or fnmatch(p.name, pat) for pat in includes
            )
            exc_ok = any(
                fnmatch(rel, pat) or fnmatch(p.name, pat) for pat in (excludes or [])
            )
            if inc_ok and not exc_ok:
                sql_files.append(p)

        # 3) Parse all files first to build dependency graph
        out_dir = Path(req.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        outputs: List[List[str]] = []
        parsed_objects: List[ObjectInfo] = []
        sql_file_map: Dict[str, List[Path]] = {}  # object_name -> [file_path,...]

        ignore_patterns: List[str] = list(getattr(self.config, "ignore", []) or [])
        
        # Helper: build canonical object key for grouping (schema.table)
        def _canon_key(name: str) -> str:
            try:
                from .openlineage_utils import sanitize_name
            except Exception:
                def sanitize_name(x):
                    return x
            s = sanitize_name(name or "")
            parts = [p for p in s.split('.') if p != ""]
            if len(parts) >= 3:
                # db.schema.table OR prefix.schema.table -> take last two segments
                return f"{parts[-2]}.{parts[-1]}"
            # Drop known prefixes like Table/View/StoredProcedure/Function
            if len(parts) >= 2 and parts[0].lower() in {"table", "view", "storedprocedure", "procedure", "function"}:
                return ".".join(parts[1:])
            return s

        # Phase 1: Parse all SQL files and collect objects
        # Store context for each object name to restore in Phase 3
        context_map: Dict[str, Dict[Path, Dict[str, Optional[str]]]] = {}
        for sql_path in sql_files:
            try:
                sql_text = read_text_safely(sql_path, encoding=req.encoding)
                # set current file for logging context (relative to sql_root)
                try:
                    parser._current_file = sql_path.relative_to(sql_root).as_posix()
                except Exception:
                    parser._current_file = str(sql_path)
                obj_info: ObjectInfo = parser.parse_sql_file(sql_text, object_hint=sql_path.stem)
                
                # IMPORTANT: Save context AFTER parsing in Phase 1, so it can be used in Phase 3
                # Context is set by _parse_procedure_string or _parse_create_procedure during parsing
                # But it may be reset after return, so we need to check if it's still set
                saved_ctx_db_phase1 = getattr(parser, '_ctx_db', None)
                saved_ctx_obj_phase1 = getattr(parser, '_ctx_obj', None)
                saved_current_db_phase1 = getattr(parser, 'current_database', None)
                
                # Debug: print context after parsing (for immediate visibility)
                # DEBUG: Uncomment to see context tracking
                # print(f"DEBUG Phase 1: After parsing {sql_path.stem}, context: _ctx_db={saved_ctx_db_phase1}, _ctx_obj={saved_ctx_obj_phase1}, current_database={saved_current_db_phase1}")
                
                # Store mapping for later processing
                raw_name = getattr(getattr(obj_info, "schema", None), "name", None) or getattr(obj_info, "name", None)
                obj_name = _canon_key(raw_name) if raw_name else None
                if obj_name:
                    # Allow multiple files to produce the same logical object (e.g., MERGE in SP outputs a table)
                    sql_file_map.setdefault(obj_name, []).append(sql_path)
                    
                    # Store context for this object name, so it can be restored in Phase 3
                    if obj_name not in context_map:
                        context_map[obj_name] = {}
                    context_map[obj_name][sql_path] = {
                        'ctx_db': saved_ctx_db_phase1,
                        'ctx_obj': saved_ctx_obj_phase1,
                        'current_db': saved_current_db_phase1
                    }
                    
                    # Skip ignored objects
                    if ignore_patterns and any(fnmatch(obj_name, pat) for pat in ignore_patterns):
                        continue
                        
                    parsed_objects.append(obj_info)
                    
            except Exception as e:
                warnings += 1
                logger.warning("failed to parse %s: %s", sql_path, e)

        # Promote soft→hard mappings before dependency resolution, allowing soft to override weak defaults
        try:
            #logger.info("DB-learn: promoting soft→hard (allowing soft to override 'infotrackerdb'/'InfoTrackerDW')")
            added = registry.promote_soft(
                min_votes=2,
                min_margin=1,
                override_weak_hard=True,
                weak_defaults=("infotrackerdb", "InfoTrackerDW"),
            )
            #logger.info(f"DB-learn: promoted/overrode {added} mappings")
            registry.save(db_map_path)
        except Exception:
            pass

        # Phase 2: Build dependency graph and resolve schemas in topological order
        dependency_graph = self._build_dependency_graph(parsed_objects)
        processing_order = self._topological_sort(dependency_graph)
        
        # Phase 3: Process objects in dependency order, building up schema registry
        resolved_objects: List[ObjectInfo] = []
        # Store temp_lineage, temp_sources, and temp_registry from ALL procedures (global, not per obj_name)
        # This allows source JSON for temp tables to use dependencies even if procedure and table have different obj_name
        # BUT: temp tables are still scoped to their procedure context (canonical naming)
        global_saved_temp_lineage: Dict[str, Dict[str, List[ColumnReference]]] = {}
        global_saved_temp_sources: Dict[str, Set[str]] = {}
        global_saved_temp_registry: Dict[str, List[str]] = {}
        for obj_name in processing_order:
            if obj_name not in sql_file_map:
                continue
            # Parse every file that contributes to this object name first, then enrich
            group_infos: List[ObjectInfo] = []
            group_paths: List[Path] = []
            # Store temp_lineage, temp_sources, and temp_registry from procedures before they get cleared (local for this obj_name)
            saved_temp_lineage: Dict[str, Dict[str, List[ColumnReference]]] = {}
            saved_temp_sources: Dict[str, Set[str]] = {}
            saved_temp_registry: Dict[str, List[str]] = {}
            for sql_path in sql_file_map[obj_name]:
                try:
                    sql_text = read_text_safely(sql_path, encoding=req.encoding)
                    try:
                        parser._current_file = sql_path.relative_to(sql_root).as_posix()
                    except Exception:
                        parser._current_file = str(sql_path)
                    # IMPORTANT: Restore context from Phase 1 for this object name
                    saved_current_db = None
                    saved_ctx_db = None
                    saved_ctx_obj = None
                    if obj_name in context_map and sql_path in context_map[obj_name]:
                        ctx_info = context_map[obj_name][sql_path]
                        saved_current_db = ctx_info.get('current_db')
                        saved_ctx_db = ctx_info.get('ctx_db')
                        saved_ctx_obj = ctx_info.get('ctx_obj')
                        # DEBUG: Uncomment to see context restoration
                        # logger.debug(f"Phase 3: Restoring context for {sql_path.stem}: _ctx_db={saved_ctx_db}, _ctx_obj={saved_ctx_obj}, current_database={saved_current_db}")
                    
                    # Reset temp registries before parsing to avoid contamination from previous passes
                    parser.temp_registry.clear()
                    parser.temp_sources.clear()
                    parser.temp_lineage.clear()
                    parser._temp_version.clear()
                    # DON'T reset context - we need it for canonical temp naming
                    # The context will be set by parse_sql_file if needed, but we preserve it from Phase 1
                    
                    # Parse the file - this will re-detect USE statement and set current_database
                    obj_info: ObjectInfo = parser.parse_sql_file(sql_text, object_hint=sql_path.stem)
                    
                    # Save temp_lineage, temp_sources, and temp_registry from procedures before they get cleared
                    if obj_info.object_type == "procedure" or "procedure" in str(sql_path).lower():
                        # Save temp_lineage, temp_sources, and temp_registry for later use (local)
                        saved_temp_lineage.update(parser.temp_lineage)
                        saved_temp_sources.update(parser.temp_sources)
                        saved_temp_registry.update(parser.temp_registry)
                        # Also save globally for source JSON creation (temp tables may be referenced from other obj_name)
                        global_saved_temp_lineage.update(parser.temp_lineage)
                        global_saved_temp_sources.update(parser.temp_sources)
                        global_saved_temp_registry.update(parser.temp_registry)
                        logger.debug(f"Phase 3: Saved temp_lineage from {sql_path.stem}: {len(saved_temp_lineage)} temp tables, keys: {list(saved_temp_lineage.keys())[:5]}")
                    
                    # Restore saved context after parse_sql_file (which may reset it)
                    # First, try to restore from Phase 1 context_map
                    if saved_current_db:
                        parser.current_database = saved_current_db
                    # Restore context for canonical temp naming
                    # parse_sql_file may set context via _parse_procedure_string or _parse_create_procedure,
                    # but if it doesn't, restore from Phase 1
                    # IMPORTANT: Always restore from Phase 1, as parse_sql_file may reset context
                    if saved_ctx_db:
                        parser._ctx_db = saved_ctx_db
                    if saved_ctx_obj:
                        parser._ctx_obj = saved_ctx_obj
                    # DEBUG: Uncomment to see context restoration
                    # logger.debug(f"Phase 3: After restoring context for {sql_path.stem}: _ctx_db={getattr(parser, '_ctx_db', None)}, _ctx_obj={getattr(parser, '_ctx_obj', None)}")
                    group_infos.append(obj_info)
                    group_paths.append(sql_path)
                except Exception as e:
                    warnings += 1
                    logger.warning("failed to parse %s: %s", sql_path, e)

            # Compute union lineage/deps across contributions to the same object (e.g., SP materializing the table)
            union_lineage: List[ColumnLineage] = []
            union_deps: Set[str] = set()
            try:
                # prefer contributions that already have lineage (procedures)
                for gi in group_infos:
                    if gi.lineage:
                        union_lineage.extend(gi.lineage)
                    if gi.dependencies:
                        union_deps.update(gi.dependencies)
            except Exception:
                pass

            # DON'T expand temp table references in union_lineage - keep them as-is
            # Temp tables that are direct sources in union_lineage (e.g., #asefl_temp) should remain in lineage
            # Only expand in column_graph.json using build_from_object_lineage, which handles multi-level expansion
            # This preserves intermediate temp tables (e.g., #insert_update_temp_asefl) in the lineage path
            expanded_union_lineage = union_lineage

            # Now emit each file, enriching table-only DDL objects with union lineage if they had none
            for obj_info, sql_path in zip(group_infos, group_paths):
                try:
                    # Register schema before emission
                    if obj_info.schema:
                        parser.schema_registry.register(obj_info.schema)
                        adapter.parser.schema_registry.register(obj_info.schema)
                        
                        # Update registry so subsequent files can resolve this object's database
                        try:
                            ns = obj_info.schema.namespace
                            nm = obj_info.schema.name
                            if ns and nm and '://' in ns:
                                db_name = ns.rsplit('/', 1)[-1]
                                # Extract schema.table from nm (may already be qualified)
                                if '.' in nm:
                                    schema_table = nm
                                else:
                                    schema_table = f"dbo.{nm}"
                                registry.learn_from_create(obj_info.object_type or "table", schema_table, db_name)
                        except Exception:
                            pass

                    # Enrich: if this is a 'table' with no lineage but a sibling provided lineage
                    if (getattr(obj_info, 'object_type', None) == 'table' and not obj_info.lineage and expanded_union_lineage):
                        obj_info.lineage = list(expanded_union_lineage)
                        if not obj_info.dependencies:
                            obj_info.dependencies = set(union_deps)

                    resolved_objects.append(obj_info)

                    ol_payload = emit_ol_from_object(
                        obj_info,
                        quality_metrics=True,
                        virtual_proc_outputs=getattr(self.config, "virtual_proc_outputs", True),
                    )

                    target = out_dir / f"{sql_path.stem}.json"
                    target.write_text(
                        json.dumps(ol_payload, indent=2, ensure_ascii=False, sort_keys=True),
                        encoding="utf-8",
                    )

                    outputs.append([str(sql_path), str(target)])

                    # Additionally, emit separate OL events for temp tables created in this file
                    # Use saved_temp_lineage/global_saved_temp_lineage instead of parser.temp_registry
                    # because parser.temp_registry is cleared before each parsing (line 301)
                    # This ensures temp tables from procedures are included even when CREATE TABLE is processed after
                    try:
                        temp_keys_set = set()
                        # First, try saved_temp_lineage (local to this obj_name)
                        for k in saved_temp_lineage.keys():
                            if isinstance(k, str) and k.startswith('#') and '@' not in k:
                                temp_keys_set.add(k)
                        # Also check global_saved_temp_lineage (for temp tables from other procedures)
                        for k in global_saved_temp_lineage.keys():
                            if isinstance(k, str) and k.startswith('#') and '@' not in k:
                                temp_keys_set.add(k)
                        # Fallback to parser.temp_registry if available (for temp tables created in this file)
                        for k in parser.temp_registry.keys():
                            if isinstance(k, str) and k.startswith('#') and '@' not in k:
                                temp_keys_set.add(k)
                        temp_keys = sorted(list(temp_keys_set))
                    except Exception:
                        temp_keys = []
                    for tmp in temp_keys:
                        try:
                            # Debug: check context before canonicalization
                            ctx_db_before = getattr(parser, '_ctx_db', None)
                            ctx_obj_before = getattr(parser, '_ctx_obj', None)
                            current_db_before = parser.current_database
                            logger.debug(f"Phase 3: Before _canonical_temp_name for {tmp}: _ctx_db={ctx_db_before}, _ctx_obj={ctx_obj_before}, current_database={current_db_before}")
                            
                            # Canonicalize temp name and derive ns
                            canonical = parser._canonical_temp_name(tmp)
                            logger.debug(f"Phase 3: After _canonical_temp_name for {tmp}: canonical={canonical}")
                            # Use _ns_and_name to get proper namespace and name format (schema.object#temp without dot before #)
                            ns_tmp, table_name = parser._ns_and_name(tmp, obj_type_hint="temp_table")
                            logger.debug(f"Phase 3: After _ns_and_name for {tmp}: namespace={ns_tmp}, name={table_name}")
                            # Build schema
                            schema = parser.schema_registry.get(ns_tmp, table_name)
                            if schema:
                                logger.debug(f"Phase 3: Found schema in registry for {tmp}: namespace={schema.namespace}, name={schema.name}")
                                # Ensure schema.name matches table_name (might be different if registered earlier)
                                if schema.name != table_name:
                                    logger.debug(f"Phase 3: Schema name mismatch! schema.name={schema.name}, table_name={table_name}, updating schema.name")
                                    schema.name = table_name
                            else:
                                logger.debug(f"Phase 3: No schema in registry for {tmp}, creating new schema with name={table_name}")
                                # Use saved_temp_registry/global_saved_temp_registry instead of parser.temp_registry
                                # because parser.temp_registry is cleared before each parsing (line 301)
                                col_names = saved_temp_registry.get(tmp, []) or global_saved_temp_registry.get(tmp, []) or parser.temp_registry.get(tmp, []) or []
                                cols = [ColumnSchema(name=c, data_type='unknown', nullable=True, ordinal=i) for i, c in enumerate(col_names)]
                                schema = TableSchema(namespace=ns_tmp, name=table_name, columns=cols)
                            # Build lineage
                            lin_list = []
                            # Use saved_temp_lineage/global_saved_temp_lineage instead of parser.temp_lineage
                            # because parser.temp_lineage is cleared before each parsing (line 303)
                            col_map = saved_temp_lineage.get(tmp) or global_saved_temp_lineage.get(tmp) or parser.temp_lineage.get(tmp) or {}
                            logger.debug(f"Phase 3: temp_lineage for {tmp}: {len(col_map)} columns, keys={list(col_map.keys())[:5] if col_map else []}")
                            # If temp_lineage is empty, or if it contains CTE references instead of real tables, try to extract sources from SQL
                            # Check if temp_lineage contains CTE references (not real tables) or if all references are empty
                            has_cte_refs = False
                            all_refs_empty = True
                            if col_map:
                                for col_name, refs in col_map.items():
                                    if refs and len(refs) > 0:
                                        all_refs_empty = False
                                    for ref in refs:
                                        # Check if ref points to a CTE (CTEs don't have namespace or have special format)
                                        # CTEs are typically not in temp_registry and don't have # prefix
                                        if ref.table_name and not ref.table_name.startswith('#') and '.' not in ref.table_name:
                                            # Might be a CTE, check if it's in cte_registry
                                            if ref.table_name in parser.cte_registry or ref.table_name.lower() in [k.lower() for k in parser.cte_registry.keys()]:
                                                has_cte_refs = True
                                                break
                                    if has_cte_refs:
                                        break
                            else:
                                all_refs_empty = True
                            
                            if (not col_map or has_cte_refs or all_refs_empty) and schema.columns:
                                # Fallback: try to extract basic lineage from SQL string for this temp table
                                logger.debug(f"Phase 3: Starting fallback for {tmp}, col_map empty={not col_map}, has_cte_refs={has_cte_refs}, schema.columns={len(schema.columns)}")
                                try:
                                    import re
                                    temp_name_simple = tmp.lstrip('#')
                                    logger.debug(f"Phase 3: Looking for SELECT ... INTO #{temp_name_simple} ... FROM ...")
                                    for sql_path in group_paths:
                                        sql_text = read_text_safely(sql_path)
                                        match = None
                                        from_table = None
                                        # Try UPDATE ... OUTPUT ... INTO #tmp pattern first (for temp tables created by UPDATE)
                                        # UPDATE may have FROM before OUTPUT or after INTO
                                        # Pattern 1: UPDATE ... FROM ... OUTPUT ... INTO #tmp
                                        pattern_update_output1 = rf'(?is)UPDATE\s+.*?\bFROM\s+([#\w]+(?:\.[#\w]+)*)[^;]*?\bOUTPUT\s+.*?\bINTO\s+#{re.escape(temp_name_simple)}\b'
                                        match_update1 = re.search(pattern_update_output1, sql_text)
                                        if match_update1:
                                            from_table = match_update1.group(1).strip('[]')
                                            logger.debug(f"Phase 3: Found UPDATE ... FROM ... OUTPUT ... INTO pattern for {tmp}, FROM={from_table}")
                                        else:
                                            # Pattern 2: UPDATE ... OUTPUT ... INTO #tmp ... FROM ...
                                            pattern_update_output2 = rf'(?is)UPDATE\s+.*?\bOUTPUT\s+.*?\bINTO\s+#{re.escape(temp_name_simple)}\b[^;]*?\bFROM\s+([#\w]+(?:\.[#\w]+)*)'
                                            match_update2 = re.search(pattern_update_output2, sql_text)
                                            if match_update2:
                                                from_table = match_update2.group(1).strip('[]')
                                                logger.debug(f"Phase 3: Found UPDATE ... OUTPUT ... INTO ... FROM pattern for {tmp}, FROM={from_table}")
                                        if match_update1 or match_update2:
                                            match_update = match_update1 or match_update2
                                        else:
                                            # Try SELECT ... INTO #tmp ... FROM ... pattern
                                            # Handle both WITH ... SELECT ... INTO and plain SELECT ... INTO
                                            # First try to find SELECT ... INTO #tmp ... FROM ... (may be after WITH clause)
                                            pattern_select = rf'(?is)SELECT\s+.*?\bINTO\s+#{re.escape(temp_name_simple)}\b[^;]*?\bFROM\s+([#\w]+(?:\.[#\w]+)*)'
                                            match = re.search(pattern_select, sql_text)
                                            if match:
                                                from_table = match.group(1).strip('[]')
                                        logger.debug(f"Phase 3: Pattern match for {tmp}: {match is not None or match_update is not None}")
                                        if from_table:
                                            # If FROM points to a CTE (not a table), look deeper in SQL for the actual source
                                            # Check if this is a CTE by looking for it in WITH clause
                                            if not from_table.startswith('#') and '.' not in from_table:
                                                # Might be a CTE, look for its definition in WITH clause
                                                # CTE definition: WITH CTEName AS (SELECT ... FROM ...)
                                                # For complex CTEs with nested parentheses, we need to find the actual table source
                                                # Strategy: find the CTE definition start, then look for FROM/JOIN with [dbo].TableName
                                                cte_start_pattern = rf'(?is)WITH\s+{re.escape(from_table)}\s+AS\s*\('
                                                cte_start_match = re.search(cte_start_pattern, sql_text)
                                                if cte_start_match:
                                                    # Find the matching closing parenthesis for the CTE definition
                                                    # This is tricky with nested parentheses, so we'll use a simpler approach:
                                                    # Look for FROM/JOIN [dbo].TableName after the CTE definition starts
                                                    cte_start_pos = cte_start_match.end()
                                                    # Look for FROM [dbo].TableName in the CTE definition (handle nested subqueries)
                                                    # Use a pattern that finds FROM/JOIN followed by [dbo].TableName
                                                    cte_from_pattern = rf'(?is)(?:FROM|JOIN)\s+\[?dbo\]?\.(\w+)'
                                                    # Search in the text after CTE definition start, but limit to reasonable length
                                                    cte_section = sql_text[cte_start_pos:cte_start_pos+2000]  # Limit search to 2000 chars
                                                    cte_from_match = re.search(cte_from_pattern, cte_section)
                                                    if cte_from_match:
                                                        from_table = f"dbo.{cte_from_match.group(1)}"
                                                        logger.debug(f"Phase 3: Found CTE, actual source is {from_table} (from CTE definition)")
                                                    else:
                                                        # Last resort: find any [dbo].TableName after CTE definition start
                                                        cte_any_pattern = rf'\[?dbo\]?\.(\w+)'
                                                        cte_any_match = re.search(cte_any_pattern, cte_section)
                                                        if cte_any_match:
                                                            from_table = f"dbo.{cte_any_match.group(1)}"
                                                            logger.debug(f"Phase 3: Found CTE, actual source is {from_table} (from CTE, any table found)")
                                                # Also check if from_table is in cte_registry and expand it
                                                if from_table and from_table.lower() in [k.lower() for k in parser.cte_registry.keys()]:
                                                    # This is a CTE - expand it to base sources
                                                    cte_name_lower = from_table.lower()
                                                    cte_name = next((k for k in parser.cte_registry.keys() if k.lower() == cte_name_lower), None)
                                                    if cte_name:
                                                        cte_info = parser.cte_registry[cte_name]
                                                        from sqlglot import expressions as exp
                                                        if isinstance(cte_info, dict) and 'definition' in cte_info:
                                                            cte_def = cte_info['definition']
                                                        elif isinstance(cte_info, exp.Select):
                                                            cte_def = cte_info
                                                        else:
                                                            cte_def = None
                                                        if cte_def and isinstance(cte_def, exp.Select):
                                                            cte_deps = parser._extract_dependencies(cte_def)
                                                            # Find first non-temp, non-CTE dependency
                                                            for cte_dep in cte_deps:
                                                                cte_dep_simple = cte_dep.split('.')[-1] if '.' in cte_dep else cte_dep
                                                                is_cte_dep_temp = cte_dep_simple.startswith('#') or (f"#{cte_dep_simple}" in parser.temp_registry)
                                                                is_cte_dep_cte = cte_dep_simple and cte_dep_simple.lower() in [k.lower() for k in parser.cte_registry.keys()]
                                                                if not is_cte_dep_temp and not is_cte_dep_cte:
                                                                    from_table = cte_dep
                                                                    logger.debug(f"Phase 3: Expanded CTE {cte_name} to base source {from_table}")
                                                                    break
                                            # Normalize table name
                                            if '.' not in from_table and not from_table.startswith('#'):
                                                from_table = f"dbo.{from_table}"
                                            # Create basic lineage for all columns
                                            ns_from, nm_from = parser._ns_and_name(from_table, obj_type_hint="table")
                                            for col in schema.columns:
                                                ref = ColumnReference(namespace=ns_from, table_name=nm_from, column_name=col.name)
                                                lin_list.append(ColumnLineage(
                                                    output_column=col.name,
                                                    input_fields=[ref],
                                                    transformation_type=TransformationType.IDENTITY,
                                                    transformation_description=f"from {nm_from}"
                                                ))
                                            logger.debug(f"Phase 3: Created fallback lineage for {tmp} from pattern: {nm_from}: {len(lin_list)} columns")
                                            break
                                except Exception as e:
                                    logger.debug(f"Phase 3: Failed to create fallback lineage for {tmp}: {e}")
                                    import traceback
                                    logger.debug(f"Phase 3: Traceback: {traceback.format_exc()}")
                            
                            for i, col in enumerate(schema.columns or []):
                                if not col_map:
                                    # No temp_lineage, but we might have created fallback lineage above
                                    # Check if we already added lineage for this column
                                    existing_lineage = [lin for lin in lin_list if lin.output_column == col.name]
                                    if not existing_lineage:
                                        # No lineage found, create empty lineage so object is included in graph
                                        lin_list.append(ColumnLineage(output_column=col.name, input_fields=[], transformation_type=TransformationType.UNKNOWN, transformation_description="temp column (no lineage)"))
                                    continue
                                refs = list(col_map.get(col.name, []))
                                # DEBUG: Uncomment to see temp table processing
                                # logger.debug(f"Phase 3: refs for {tmp}.{col.name}: {len(refs)} references")
                                # Normalize temp table references to use new format (schema.object#temp)
                                normalized_refs = []
                                # Build temp_name_map for this temp table
                                ref_ns, ref_name = parser._ns_and_name(tmp, obj_type_hint="temp_table")
                                temp_name_map_local = {}
                                # Map various old formats to new canonical format
                                temp_part = tmp.lstrip('#')
                                temp_name_map_local[f"dbo.#{temp_part}"] = ref_name
                                temp_name_map_local[f"#{temp_part}"] = ref_name
                                try:
                                    ns_db = ref_ns.rsplit('/', 1)[1] if ref_ns else None
                                    if ns_db:
                                        temp_name_map_local[f"{ns_db}.dbo.#{temp_part}"] = ref_name
                                        temp_name_map_local[f"{ns_db}.#{temp_part}"] = ref_name
                                except Exception:
                                    pass
                                
                                for ref in refs:
                                    # If this reference points to the same temp table, update it to new format
                                    should_normalize = False
                                    ref_table = ref.table_name
                                    if ref_table and '#' in ref_table:
                                        # Check if ref.table_name matches any old format in temp_name_map
                                        # Try different variants: with DB prefix, without DB prefix, with schema, without schema
                                        variants = [ref_table]
                                        try:
                                            ref_db = ref.namespace.rsplit('/', 1)[1] if ref.namespace else None
                                            if ref_db and not ref_table.startswith(f"{ref_db}."):
                                                variants.append(f"{ref_db}.{ref_table}")
                                            if ref_table.startswith(f"{ref_db}.") if ref_db else False:
                                                variants.append(ref_table[len(ref_db) + 1:])
                                            # Try without schema (just #temp)
                                            if '.' in ref_table:
                                                temp_part_ref = ref_table.split('#')[-1] if '#' in ref_table else None
                                                if temp_part_ref:
                                                    variants.append(f"#{temp_part_ref}")
                                                    if ref_db:
                                                        variants.append(f"{ref_db}.#{temp_part_ref}")
                                        except Exception:
                                            pass
                                        
                                        # Check each variant
                                        for variant in variants:
                                            if variant in temp_name_map_local:
                                                ref_table = temp_name_map_local[variant]
                                                should_normalize = True
                                                break
                                        
                                        # Also check if it's the same temp table by simple name
                                        if not should_normalize:
                                            temp_name_simple = tmp.lstrip('#')
                                            if (tmp in ref_table or f"#{temp_name_simple}" in ref_table or 
                                                ref_table.endswith(tmp) or ref_table == tmp or ref_table == temp_name_simple):
                                                should_normalize = True
                                    
                                    if should_normalize:
                                        # This is a self-reference to the temp table - skip it, we want base sources only
                                        # Don't add self-references to lineage
                                        continue
                                    else:
                                        # Keep other references as-is (these are base sources)
                                        normalized_refs.append(ref)
                                if normalized_refs:
                                    lin_list.append(ColumnLineage(output_column=col.name, input_fields=normalized_refs, transformation_type=TransformationType.IDENTITY, transformation_description="from temp source select"))
                                else:
                                    lin_list.append(ColumnLineage(output_column=col.name, input_fields=[], transformation_type=TransformationType.UNKNOWN, transformation_description="temp column"))
                            # Use saved_temp_sources/global_saved_temp_sources instead of parser.temp_sources
                            # because parser.temp_sources is cleared before each parsing (line 302)
                            deps = set(saved_temp_sources.get(tmp, set()) or global_saved_temp_sources.get(tmp, set()) or parser.temp_sources.get(tmp, set()) or set())
                            temp_obj = ObjectInfo(name=table_name, object_type="temp_table", schema=schema, lineage=lin_list, dependencies=deps)
                            logger.debug(f"Phase 3: Created temp_obj for {tmp}: obj.name={temp_obj.name}, obj.schema.name={temp_obj.schema.name}")
                            # Include in graph
                            resolved_objects.append(temp_obj)
                            # Write OL JSON
                            # Extract just db.schema.#temp for filename (skip middle object context)
                            parts = canonical.split('.')
                            if len(parts) >= 3 and parts[-1].startswith('#'):
                                # Format: DB.schema.object.#temp or longer -> use DB.schema.#temp
                                db_part = parts[0]
                                schema_part = parts[1] if len(parts) > 1 else 'dbo'
                                temp_part = parts[-1]
                                safe_name = f"{db_part}.{schema_part}.{temp_part}"
                            else:
                                safe_name = canonical
                            safe = safe_name.replace('/', '_').replace('\\', '_').replace(':', '_').replace('#', 'hash')
                            tpath = out_dir / f"{sql_path.stem}__temp__{safe}.json"
                            tpayload = emit_ol_from_object(temp_obj, quality_metrics=True, virtual_proc_outputs=getattr(self.config, "virtual_proc_outputs", True))
                            tpath.write_text(json.dumps(tpayload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
                            outputs.append([str(sql_path), str(tpath)])
                        except Exception:
                            pass

                    # Optionally emit minimal source dataset events for inputs that do not have their own outputs
                    if self._emit_external_sources:
                        try:
                            # Build temp_name_map from all resolved objects for normalizing temp table names
                            temp_name_map_sources = {}
                            temp_obj_map_sources = {}  # Map normalized name -> temp_obj for getting dependencies
                            temp_sources_map = {}  # Map normalized name -> Set[str] dependencies from saved_temp_sources
                            for resolved_obj in resolved_objects:
                                if resolved_obj.object_type == "temp_table" and resolved_obj.schema and resolved_obj.schema.name:
                                    normalized_name = resolved_obj.schema.name
                                    try:
                                        ns_db = resolved_obj.schema.namespace.rsplit('/', 1)[1] if resolved_obj.schema.namespace else None
                                        if ns_db and normalized_name.startswith(f"{ns_db}."):
                                            normalized_name = normalized_name[len(ns_db) + 1:]
                                    except Exception:
                                        pass
                                    if '#' in normalized_name:
                                        temp_part = normalized_name.split('#')[-1]
                                        temp_name_map_sources[f"dbo.#{temp_part}"] = normalized_name
                                        temp_name_map_sources[f"#{temp_part}"] = normalized_name
                                        if ns_db:
                                            temp_name_map_sources[f"{ns_db}.dbo.#{temp_part}"] = normalized_name
                                            temp_name_map_sources[f"{ns_db}.#{temp_part}"] = normalized_name
                                        # Store temp_obj for later use
                                        temp_obj_map_sources[normalized_name] = resolved_obj
                            
                            # Also use saved_temp_sources from procedures (may contain temp tables not in resolved_objects)
                            # Use global_saved_temp_sources to include temp tables from all procedures, not just this obj_name
                            # This allows source JSON for temp tables to use dependencies even if procedure and table have different obj_name
                            for temp_key, deps in global_saved_temp_sources.items():
                                if temp_key.startswith('#'):
                                    # Try to find canonical name for this temp table
                                    temp_part = temp_key.lstrip('#')
                                    # Try to find matching normalized_name from temp_name_map_sources
                                    normalized_name = None
                                    for key, val in temp_name_map_sources.items():
                                        if key.endswith(f"#{temp_part}") or key == f"#{temp_part}":
                                            normalized_name = val
                                            break
                                    if not normalized_name:
                                        # Use canonical name if available
                                        try:
                                            canonical = parser._canonical_temp_name(temp_key)
                                            ns_tmp, table_name = parser._ns_and_name(temp_key, obj_type_hint="temp_table")
                                            try:
                                                ns_db = ns_tmp.rsplit('/', 1)[1] if ns_tmp else None
                                                if ns_db and table_name.startswith(f"{ns_db}."):
                                                    normalized_name = table_name[len(ns_db) + 1:]
                                                else:
                                                    normalized_name = table_name
                                            except Exception:
                                                normalized_name = table_name
                                        except Exception:
                                            normalized_name = f"dbo.{temp_key}"
                                    if normalized_name and deps:
                                        temp_sources_map[normalized_name] = deps
                                        # Also add to temp_name_map_sources if not already there
                                        if normalized_name not in temp_name_map_sources.values():
                                            temp_name_map_sources[f"dbo.{temp_key}"] = normalized_name
                                            temp_name_map_sources[temp_key] = normalized_name
                            
                            inputs = ol_payload.get('inputs') or []
                            if inputs:
                                src_dir = out_dir / "sources"
                                src_dir.mkdir(parents=True, exist_ok=True)
                                for inp in inputs:
                                    ns_in = inp.get('namespace')
                                    nm_in = inp.get('name')
                                    if not nm_in:
                                        continue
                                    # Skip variables only (keep temp datasets visible)
                                    s = str(nm_in)
                                    if s.startswith('@'):
                                        continue
                                    
                                    # Normalize temp table names using temp_name_map
                                    normalized_s = s
                                    if '#' in s and temp_name_map_sources:
                                        # Try different variants
                                        variants = [s]
                                        try:
                                            ns_db = ns_in.rsplit('/', 1)[1] if ns_in else None
                                            if ns_db and not s.startswith(f"{ns_db}."):
                                                variants.append(f"{ns_db}.{s}")
                                            if s.startswith(f"{ns_db}.") if ns_db else False:
                                                variants.append(s[len(ns_db) + 1:])
                                            if '.' in s:
                                                temp_part = s.split('#')[-1] if '#' in s else None
                                                if temp_part:
                                                    variants.append(f"#{temp_part}")
                                                    if ns_db:
                                                        variants.append(f"{ns_db}.#{temp_part}")
                                        except Exception:
                                            pass
                                        
                                        for variant in variants:
                                            if variant in temp_name_map_sources:
                                                normalized_s = temp_name_map_sources[variant]
                                                break
                                    
                                    # filename-safe
                                    safe = normalized_s.replace('/', '_').replace('\\', '_').replace(':', '_')
                                    src_path = src_dir / f"src_{safe}.json"
                                    if src_path.exists():
                                        continue
                                    
                                    # For temp tables, get dependencies from temp_obj_map_sources or temp_sources_map
                                    src_inputs = []
                                    if '#' in normalized_s:
                                        # This is a temp table - find its ObjectInfo in temp_obj_map_sources
                                        # Try exact match first, then try variants
                                        temp_obj = temp_obj_map_sources.get(normalized_s)
                                        if not temp_obj:
                                            # Try to find by matching temp part
                                            temp_part = normalized_s.split('#')[-1] if '#' in normalized_s else ""
                                            for key, obj in temp_obj_map_sources.items():
                                                if '#' in key and key.split('#')[-1] == temp_part:
                                                    temp_obj = obj
                                                    logger.debug(f"Phase 3: Found temp_obj for {normalized_s} by temp_part {temp_part}: {key}")
                                                    break
                                        
                                        # Get dependencies from temp_obj or temp_sources_map
                                        deps = None
                                        if temp_obj and temp_obj.dependencies:
                                            deps = temp_obj.dependencies
                                            logger.debug(f"Phase 3: Using dependencies from temp_obj for {normalized_s}: {len(deps)} deps")
                                        elif normalized_s in temp_sources_map:
                                            deps = temp_sources_map[normalized_s]
                                            logger.debug(f"Phase 3: Using dependencies from temp_sources_map for {normalized_s}: {len(deps)} deps")
                                        else:
                                            # Try to find by temp_part in temp_sources_map or global_saved_temp_sources
                                            temp_part = normalized_s.split('#')[-1] if '#' in normalized_s else ""
                                            temp_key = f"#{temp_part}"
                                            if temp_key in global_saved_temp_sources:
                                                deps = global_saved_temp_sources[temp_key]
                                                logger.debug(f"Phase 3: Using dependencies from global_saved_temp_sources for {normalized_s} (temp_key={temp_key}): {len(deps)} deps")
                                        
                                        if deps:
                                            from ..lineage import _ns_for_dep, _strip_db_prefix
                                            for dep in sorted(deps):
                                                dep_ns = _ns_for_dep(dep, ns_in or "mssql://localhost/InfoTrackerDW")
                                                dep_name = _strip_db_prefix(dep)
                                                src_inputs.append({"namespace": dep_ns, "name": dep_name})
                                        else:
                                            logger.debug(f"Phase 3: No dependencies found for {normalized_s}, temp_obj_map_sources keys: {list(temp_obj_map_sources.keys())[:5]}, temp_sources_map keys: {list(temp_sources_map.keys())[:5]}")
                                    
                                    # Minimal OL event for source dataset
                                    src_event = {
                                        "eventType": "COMPLETE",
                                        "eventTime": datetime.now().isoformat()[:19] + "Z",
                                        "run": {"runId": "00000000-0000-0000-0000-000000000000"},
                                        "job": {"namespace": "infotracker/sources", "name": f"source/{normalized_s}"},
                                        "inputs": src_inputs,
                                        "outputs": [{"namespace": ns_in or adapter.parser.schema_registry.get(None, None) or "mssql://localhost/InfoTrackerDW", "name": normalized_s, "facets": {}}],
                                    }
                                    src_path.write_text(json.dumps(src_event, indent=2, ensure_ascii=False, sort_keys=True), encoding='utf-8')
                        except Exception:
                            pass

                    # Check for warnings with enhanced diagnostics
                    out0 = (ol_payload.get("outputs") or [])
                    out0 = out0[0] if out0 else {}
                    facets = out0.get("facets", {})
                    has_schema_fields = bool(facets.get("schema", {}).get("fields"))
                    has_col_lineage = bool(facets.get("columnLineage", {}).get("fields"))

                    # Enhanced warning classification
                    warning_reason = None
                    if getattr(obj_info, "object_type", "unknown") == "unknown":
                        warning_reason = "UNKNOWN_OBJECT_TYPE"
                    elif hasattr(obj_info, 'no_output_reason') and obj_info.no_output_reason:
                        warning_reason = obj_info.no_output_reason
                    elif not (has_schema_fields or has_col_lineage):
                        warning_reason = "NO_SCHEMA_OR_LINEAGE"

                    if warning_reason:
                        warnings += 1
                        try:
                            disp = f"{obj_info.schema.namespace}.{obj_info.schema.name}" if getattr(obj_info, 'schema', None) else obj_info.name
                        except Exception:
                            disp = obj_info.name
                        logger.warning("Object %s: %s", disp, warning_reason)

                except Exception as e:
                    warnings += 1
                    logger.warning("failed to process %s: %s", sql_path, e)

        # 4) Build column graph from resolved objects (second pass)
        if resolved_objects:
            try:
                graph = ColumnGraph()
                graph.build_from_object_lineage(resolved_objects)  # Use resolved objects with expanded schemas
                self._column_graph = graph

                # Save graph to disk for impact analysis
                graph_path = Path(req.out_dir) / "column_graph.json"
                edges_dump = []
                seen = set()
                for edges_list in graph._downstream_edges.values():
                    for e in edges_list:
                        key = (str(e.from_column), str(e.to_column),
                            getattr(e.transformation_type, "value", str(e.transformation_type)),
                            e.transformation_description or "")
                        if key in seen:
                            continue
                        seen.add(key)
                        edges_dump.append({
                            "from": str(e.from_column),
                            "to": str(e.to_column),
                            "transformation": key[2],
                            "description": key[3],
                        })
                # Also include nodes (columns) even if they don't have edges
                nodes_dump = []
                for node in graph._nodes.values():
                    nodes_dump.append({
                        "namespace": node.namespace,
                        "table": node.table_name,
                        "column": node.column_name,
                    })
                graph_path.write_text(json.dumps({"edges": edges_dump, "nodes": nodes_dump}, indent=2, ensure_ascii=False), encoding="utf-8")
                # Persist learned object→DB mapping for future runs
                try:
                    registry.save(db_map_path)
                except Exception:
                    pass
            except Exception as e:
                logger.warning("failed to build column graph: %s", e)


        return {
            "columns": ["input_sql", "openlineage_json"],
            "rows": outputs,     # lista list – _emit to obsługuje
            "warnings": warnings,
        }

    def _apply_dbt_context(self, sql_dir: Path, adapter) -> None:
        """If dbt_project.yml is present near sql_dir, use its defaults.

        We read vars.default_database and vars.default_schema and, if not already
        provided in config, set parser defaults accordingly. This keeps behavior
        non-intrusive for classic SQL mode.
        """
        # Locate dbt_project.yml in sql_dir or its parent(s)
        candidates = [
            Path(sql_dir) / 'dbt_project.yml',
            Path(sql_dir).parent / 'dbt_project.yml',
        ]
        project = next((p for p in candidates if p.exists()), None)
        if not project:
            return
        data = yaml.safe_load(project.read_text(encoding='utf-8')) or {}
        vars_cfg = data.get('vars', {}) or {}
        db = vars_cfg.get('default_database')
        sch = vars_cfg.get('default_schema')
        # Apply only if not set in config to allow explicit overrides
        try:
            if db and not getattr(self.config, 'default_database', None):
                self.config.default_database = db
                if hasattr(adapter, 'parser'):
                    adapter.parser.set_default_database(db)
            if sch and not getattr(self.config, 'default_schema', None):
                self.config.default_schema = sch
                if hasattr(adapter, 'parser') and hasattr(adapter.parser, 'set_default_schema'):
                    adapter.parser.set_default_schema(sch)
        except Exception:
            pass

    def _build_dependency_graph(self, objects: List[ObjectInfo]) -> Dict[str, Set[str]]:
        """Build dependency graph: object_name -> set of dependencies.
        
        Temp tables are now included as normal nodes in the graph with their canonical names (dbo.#name).
        """
        dependencies: Dict[str, Set[str]] = {}

        # Helper: normalize a name to our object key space (schema.table)
        def _dequote(s: str) -> str:
            try:
                import re
                return re.sub(r"[\[\]\"'`]", "", s or "").strip()
            except Exception:
                return (s or "").strip()

        def _strip_db(name: str) -> str:
            name = _dequote(name or "")
            parts = (name or "").split('.')
            return '.'.join(parts[-2:]) if len(parts) >= 2 else (name or "")

        def _is_noise(n: str) -> bool:
            """Check if a name is noise (variables, dynamic tokens, but NOT temp tables)."""
            if not n:
                return True
            s = n.strip()
            # Variables (@@, @var)
            if s.startswith('@'):
                return True
            # Dynamic string concatenation
            if '+' in s:
                return True
            # Bracket-only tokens without dot (malformed identifiers)
            if s.startswith('[') and s.endswith(']') and '.' not in s:
                return True
            # Temp tables are NOT noise - they're legitimate dependencies
            return False

        # Build case-insensitive key map for objects
        key_map: Dict[str, str] = {}
        for obj in objects:
            k = _dequote(obj.schema.name if obj.schema else obj.name)
            # Canonical key: schema.table (including dbo.#temp for temp tables)
            canon = _strip_db(k)
            key_map[canon.lower()] = canon
            # If an object came with DB prefix, also map the 3-part form to canonical
            if k.count('.') >= 2:
                key_map[k.lower()] = canon

        for obj in objects:
            obj_name = _strip_db(_dequote(obj.schema.name if obj.schema else obj.name))
            deps: Set[str] = set()

            # Prefer explicit ObjectInfo.dependencies
            raw_deps = set(obj.dependencies) if obj.dependencies else set()
            if not raw_deps:
                # Fallback to lineage input fields
                for ln in obj.lineage or []:
                    for f in ln.input_fields or []:
                        raw_deps.add(f.table_name)

            # Filter raw deps and map to known objects
            for d in raw_deps:
                if _is_noise(d):
                    continue
                norm = _strip_db(d).lower()
                if norm == obj_name.lower():
                    continue
                # include only if dependency is among parsed objects
                if norm in key_map:
                    deps.add(key_map[norm])

            # If explicit deps yielded nothing (e.g., only temps), try lineage inputs as a secondary fallback
            if not deps and obj.lineage:
                for ln in obj.lineage:
                    for f in ln.input_fields or []:
                        nm2 = f.table_name
                        if _is_noise(nm2):
                            continue
                        norm2 = _strip_db(nm2).lower()
                        if norm2 == obj_name.lower():
                            continue
                        if norm2 in key_map:
                            deps.add(key_map[norm2])
            dependencies[obj_name] = deps

        return dependencies
    
    def _topological_sort(self, dependencies: Dict[str, Set[str]]) -> List[str]:
        """Sort objects in dependency order (dependencies first)."""
        result = []
        remaining = dependencies.copy()
        
        while remaining:
            # Find nodes with no dependencies (or dependencies already processed)
            ready = []
            for node, deps in remaining.items():
                if not deps or all(dep in result for dep in deps):
                    ready.append(node)
            
            if not ready:
                # Circular dependency or missing dependency - process remaining arbitrarily
                ready = [next(iter(remaining.keys()))]
                logger.info("Circular or missing dependencies detected, processing: %s", ready[0])
            
            # Process ready nodes
            for node in ready:
                result.append(node)
                del remaining[node]
        
        return result

    # ------------------ IMPACT (prosty wariant; zostaw swój jeśli masz bogatszy) ------------------

    def run_impact(self, req: ImpactRequest) -> Dict[str, Any]:
        """
        Zwraca krawędzie upstream/downstream dla wskazanej kolumny.
        Selector akceptuje:
        - 'dbo.table.column' (zalecane),
        - 'table.column' (dokleimy domyślne 'dbo'),
        - pełny klucz 'namespace.table.column' dokładnie jak w grafie.
        """
        if not self._column_graph:
            # spróbuj wczytać z dysku (ten sam out_dir, co w extract)
            try:
                graph_dir = req.graph_dir if req.graph_dir else Path(getattr(self.config, "out_dir", "build/lineage"))
                graph_path = graph_dir / "column_graph.json"
                if graph_path.exists():
                    data = json.loads(graph_path.read_text(encoding="utf-8"))
                    graph = ColumnGraph()
                    import re as _re
                    pat = _re.compile(r'^(mssql://localhost/[^.]+)\.(.+)\.([^.]+)$')
                    for edge in data.get("edges", []):
                        mf = pat.match(edge.get("from", ""))
                        mt = pat.match(edge.get("to", ""))
                        if not (mf and mt):
                            # Skip malformed entries gracefully
                            continue
                        from_ns, from_tbl, from_col = mf.group(1), mf.group(2), mf.group(3)
                        to_ns, to_tbl, to_col = mt.group(1), mt.group(2), mt.group(3)
                        graph.add_edge(ColumnEdge(
                            from_column=ColumnNode(from_ns, from_tbl, from_col),
                            to_column=ColumnNode(to_ns, to_tbl, to_col),
                            transformation_type=TransformationType(edge.get("transformation", "IDENTITY")),
                            transformation_description=edge.get("description", ""),
                        ))
                    self._column_graph = graph
            except Exception as e:
                logger.warning("failed to load column graph from disk: %s", e)

        if not self._column_graph:
            return {"columns": ["message"],
                    "rows": [["Column graph is not built. Run 'extract' first."]]}


        sel = req.selector.strip()

        # Parse direction from + symbols in selector
        direction_downstream = False
        direction_upstream = False
        
        if sel.startswith('+') and sel.endswith('+'):
            # +column+ → both directions
            direction_downstream = True
            direction_upstream = True
            sel = sel[1:-1]  # remove both + symbols
        elif sel.startswith('+'):
            # +column → upstream only
            direction_upstream = True
            sel = sel[1:]  # remove + from start
        elif sel.endswith('+'):
            # column+ → downstream only
            direction_downstream = True
            sel = sel[:-1]  # remove + from end
        else:
            # column → default (downstream)
            direction_downstream = True

        # Normalizacja selektora - obsługuj różne formaty:
        # 1. table.column -> dbo.table.column (legacy)
        # 2. schema.table.column -> schema.table.column (legacy)
        # 3. database.schema.table.column -> namespace/database.schema.table.column  
        # 4. database.schema.table.* -> namespace/database.schema.table.* (table wildcard)
        # 5. ..column -> ..column (column wildcard)
        # 6. pełny URI -> użyj jak jest
        if "://" in sel:
            # pełny URI, użyj jak jest
            pass
        elif sel.startswith('.') and not sel.startswith('..'):
            # Alias: .column -> ..column (column wildcard in default namespace)
            sel = f"mssql://localhost/InfoTrackerDW..{sel[1:]}"
        elif sel.startswith('..'):
            # Column wildcard pattern - leave as is, will be handled specially
            sel = f"mssql://localhost/InfoTrackerDW{sel}"
        elif sel.endswith('.*'):
            # Table wildcard pattern: keep as provided and let ColumnGraph handle suffix matching
            base_sel = sel[:-2]  # Remove .*
            parts = [p for p in base_sel.split('.') if p]
            if len(parts) not in (2, 3) and '://' not in base_sel:
                return {
                    "columns": ["message"],
                    "rows": [[f"Unsupported wildcard selector format: '{req.selector}'. Use 'schema.table.*' or 'database.schema.table.*'."]],
                }
            # leave sel unchanged for find_columns_wildcard
        else:
            parts = [p for p in sel.split(".") if p]
            if len(parts) == 2:
                # table.column -> namespace/dbo.table.column
                sel = f"mssql://localhost/InfoTrackerDW.dbo.{parts[0]}.{parts[1]}"
            elif len(parts) == 3:
                # schema.table.column -> namespace/schema.table.column
                sel = f"mssql://localhost/InfoTrackerDW.{sel}"
            elif len(parts) == 4:
                # database.schema.table.column -> host/database.schema.table.column (no default DB)
                sel = f"mssql://localhost/{sel}"
            else:
                return {
                    "columns": ["message"],
                    "rows": [[f"Unsupported selector format: '{req.selector}'. Use 'table.column', 'schema.table.column', 'database.schema.table.column', 'database.schema.table.*' (table wildcard), '..columnname' (column wildcard), '.columnname' (alias), or full URI."]],
                }

        target = self._column_graph.find_column(sel)
        targets = []
        
        # Check if this is a wildcard selector
        if '*' in sel or '..' in sel or sel.endswith('.*'):
            targets = self._column_graph.find_columns_wildcard(sel)
            if not targets:
                return {
                    "columns": ["message"],
                    "rows": [[f"No columns found matching pattern '{sel}'."]],
                }
        else:
            # Single column selector
            if not target:
                return {
                    "columns": ["message"],
                    "rows": [[f"Column '{sel}' not found in graph."]],
                }
            targets = [target]

        # Compute BFS levels from the target(s) for topological sorting
        # Build combined (min) distance maps for multi-target selections
        def _merge_min(dst: Dict[str, int], src: Dict[str, int]):
            for k, v in (src or {}).items():
                if k not in dst or v < dst[k]:
                    dst[k] = v

        dist_up_all: Dict[str, int] = {}
        dist_dn_all: Dict[str, int] = {}

        if targets:
            for t in targets:
                try:
                    du = self._column_graph.distances_upstream(t, req.max_depth or 0)
                    dd = self._column_graph.distances_downstream(t, req.max_depth or 0)
                    _merge_min(dist_up_all, du)
                    _merge_min(dist_dn_all, dd)
                except Exception:
                    continue

        rows_with_level: List[tuple] = []  # (level, direction, from, to, transform, desc)

        def edge_row(direction: str, e) -> None:
            # For impact output, normalize CAST/CASE to 'expression' per UX request
            def _impact_transform_label(tt) -> str:
                v = getattr(tt, "value", str(tt))
                try:
                    up = str(v).upper()
                    if up in ("CAST", "CASE"):
                        return "expression"
                except Exception:
                    pass
                return v
            from_s = str(e.from_column)
            to_s = str(e.to_column)
            # Determine topological level based on direction
            if direction == "downstream":
                lvl = dist_dn_all.get(to_s.lower(), None)
            else:
                lvl = dist_up_all.get(from_s.lower(), None)
            rows_with_level.append((
                (999999 if lvl is None else int(lvl)),
                direction,
                from_s,
                to_s,
                _impact_transform_label(e.transformation_type),
                e.transformation_description or "",
            ))

        # Process all target columns
        for target in targets:
            if direction_upstream:
                for e in self._column_graph.get_upstream(target, req.max_depth):
                    edge_row("upstream", e)
            if direction_downstream:
                for e in self._column_graph.get_downstream(target, req.max_depth):
                    edge_row("downstream", e)

        # Sort rows topologically by (level, direction, from, to)
        rows_with_level.sort(key=lambda r: (r[0], r[1], r[2], r[3]))

        # Remove duplicates while preserving order
        seen = set()
        unique_rows: List[List[str]] = []
        for lvl, direction, from_s, to_s, transf, desc in rows_with_level:
            key_tuple = (from_s, to_s, direction, transf, desc)
            if key_tuple in seen:
                continue
            seen.add(key_tuple)
            level_str = "" if lvl is None else str(lvl)
            unique_rows.append([from_s, to_s, direction, transf, desc, level_str])

        if not unique_rows:
            # Show info about the matched columns
            if len(targets) == 1:
                unique_rows = [[str(targets[0]), str(targets[0]), "info", "", "No relationships found", ""]]
            else:
                unique_rows = [[f"Matched {len(targets)} columns", "", "info", "", f"Pattern: {req.selector}", ""]]

        return {
            "columns": ["from", "to", "direction", "transformation", "description", "level"],
            "rows": unique_rows,
        }


    # ------------------ DIFF (updated implementation) ------------------

    def run_diff(self, base_dir: Path, head_dir: Path, format: str, **kwargs) -> Dict[str, Any]:
        """
        Compare base and head OpenLineage artifacts to detect breaking changes.
        
        Args:
            base_dir: Directory containing base OpenLineage JSON artifacts
            head_dir: Directory containing head OpenLineage JSON artifacts  
            format: Output format (text|json)
            **kwargs: Additional options including 'threshold' to override config
            
        Returns:
            Dict with results including exit_code (1 if breaking changes, 0 otherwise)
        """
        from .openlineage_utils import OpenLineageLoader, OLMapper
        from .diff import BreakingChangeDetector, Severity
        
        try:
            # Load OpenLineage artifacts from both directories
            base_artifacts = OpenLineageLoader.load_dir(base_dir)
            head_artifacts = OpenLineageLoader.load_dir(head_dir)
            
            # Convert to ObjectInfo instances
            base_objects = OLMapper.to_object_infos(base_artifacts)
            head_objects = OLMapper.to_object_infos(head_artifacts)
            
            # Detect changes
            detector = BreakingChangeDetector()
            report = detector.compare(base_objects, head_objects)
            
            # Use threshold from CLI flag if provided, otherwise from config
            threshold = (kwargs.get('threshold') or self.config.severity_threshold).upper()
            filtered_changes = []
            
            if threshold == "BREAKING":
                # Only show BREAKING changes
                filtered_changes = [c for c in report.changes if c.severity == Severity.BREAKING]
            elif threshold == "POTENTIALLY_BREAKING":
                # Show BREAKING and POTENTIALLY_BREAKING changes
                filtered_changes = [c for c in report.changes if c.severity in [Severity.BREAKING, Severity.POTENTIALLY_BREAKING]]
            else:  # NON_BREAKING
                # Show all changes
                filtered_changes = report.changes
            
            # Determine exit code based on threshold
            exit_code = 0
            if threshold == "BREAKING":
                exit_code = 1 if any(c.severity == Severity.BREAKING for c in report.changes) else 0
            elif threshold == "POTENTIALLY_BREAKING":
                exit_code = 1 if any(c.severity in [Severity.BREAKING, Severity.POTENTIALLY_BREAKING] for c in report.changes) else 0
            else:  # NON_BREAKING
                exit_code = 1 if len(report.changes) > 0 else 0
            
            # Build filtered report
            if filtered_changes:
                filtered_rows = []
                for change in filtered_changes:
                    filtered_rows.append([
                        change.object_name,
                        change.column_name or "",
                        change.change_type.value,
                        change.severity.value,
                        change.description
                    ])
            else:
                filtered_rows = []
            
            return {
                "columns": ["object", "column", "change_type", "severity", "description"],
                "rows": filtered_rows,
                "exit_code": exit_code,
                "summary": {
                    "total_changes": len(filtered_changes),
                    "breaking_changes": len([c for c in filtered_changes if c.severity.value == "BREAKING"]),
                    "potentially_breaking": len([c for c in filtered_changes if c.severity.value == "POTENTIALLY_BREAKING"]),
                    "non_breaking": len([c for c in filtered_changes if c.severity.value == "NON_BREAKING"])
                }
            }
            
        except Exception as e:
            logger.error(f"Error running diff: {e}")
            return {
                "error": str(e),
                "columns": ["message"], 
                "rows": [["Error running diff: " + str(e)]], 
                "exit_code": 1
            }

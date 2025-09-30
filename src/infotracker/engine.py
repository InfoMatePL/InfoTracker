# src/infotracker/engine.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
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
    max_depth: int = 2
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

        # 2) Include/Exclude (listy)
        def match_any(p: Path, patterns: Optional[List[str]]) -> bool:
            if not patterns:
                return True
            return any(p.match(g) for g in patterns)

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
        sql_files = [
            p for p in sorted(sql_root.rglob("*.sql"))
            if match_any(p, includes) and not match_any(p, excludes)
        ]

        # 3) Parse all files first to build dependency graph
        out_dir = Path(req.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        outputs: List[List[str]] = []
        parsed_objects: List[ObjectInfo] = []
        sql_file_map: Dict[str, List[Path]] = {}  # object_name -> [file_path,...]

        ignore_patterns: List[str] = list(getattr(self.config, "ignore", []) or [])
        
        # Phase 1: Parse all SQL files and collect objects
        for sql_path in sql_files:
            try:
                sql_text = read_text_safely(sql_path, encoding=req.encoding)
                obj_info: ObjectInfo = parser.parse_sql_file(sql_text, object_hint=sql_path.stem)
                
                # Store mapping for later processing
                obj_name = getattr(getattr(obj_info, "schema", None), "name", None) or getattr(obj_info, "name", None)
                if obj_name:
                    # Allow multiple files to produce the same logical object (e.g., MERGE in SP outputs a table)
                    sql_file_map.setdefault(obj_name, []).append(sql_path)
                    
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
        for obj_name in processing_order:
            if obj_name not in sql_file_map:
                continue
            # Process every file that contributes to this object name
            for sql_path in sql_file_map[obj_name]:
                try:
                    sql_text = read_text_safely(sql_path, encoding=req.encoding)

                    # Parse with updated schema registry (now has dependencies resolved)
                    obj_info: ObjectInfo = parser.parse_sql_file(sql_text, object_hint=sql_path.stem)
                    resolved_objects.append(obj_info)

                    # Register this object's schema for future dependencies
                    if obj_info.schema:
                        parser.schema_registry.register(obj_info.schema)
                        # Also register in adapter's parser for lineage generation
                        adapter.parser.schema_registry.register(obj_info.schema)

                    # Generate OpenLineage directly from resolved ObjectInfo
                    ol_payload = emit_ol_from_object(
                        obj_info,
                        quality_metrics=True,
                        virtual_proc_outputs=getattr(self.config, "virtual_proc_outputs", True),
                    )

                    # Save to file
                    target = out_dir / f"{sql_path.stem}.json"
                    target.write_text(
                        json.dumps(ol_payload, indent=2, ensure_ascii=False, sort_keys=True),
                        encoding="utf-8",
                    )

                    outputs.append([str(sql_path), str(target)])

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
                        logger.warning("Object %s: %s", obj_info.name, warning_reason)

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
                graph_path.write_text(json.dumps({"edges": edges_dump}, indent=2, ensure_ascii=False), encoding="utf-8")
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

    def _build_dependency_graph(self, objects: List[ObjectInfo]) -> Dict[str, Set[str]]:
        """Build dependency graph: object_name -> set of dependencies."""
        dependencies = {}
        
        for obj in objects:
            obj_name = obj.schema.name if obj.schema else obj.name
            
            # Use ObjectInfo.dependencies first
            if obj.dependencies:
                dependencies[obj_name] = set(obj.dependencies)
            else:
                # Fallback to extracting dependencies from lineage.input_fields
                dependencies[obj_name] = set()
                for lineage in obj.lineage:
                    for input_field in lineage.input_fields:
                        dep_name = input_field.table_name
                        if dep_name != obj_name:  # Don't depend on self
                            dependencies[obj_name].add(dep_name)
        
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
                # database.schema.table.column -> namespace/database.schema.table.column  
                sel = f"mssql://localhost/InfoTrackerDW.{sel}"
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

        rows: List[List[str]] = []

        def edge_row(direction: str, e) -> List[str]:
            return [
                str(e.from_column), 
                str(e.to_column), 
                direction,
                getattr(e.transformation_type, "value", str(e.transformation_type)),
                e.transformation_description or "",
            ]

        # Process all target columns
        for target in targets:
            if direction_upstream:
                for e in self._column_graph.get_upstream(target, req.max_depth):
                    rows.append(edge_row("upstream", e))
            if direction_downstream:
                for e in self._column_graph.get_downstream(target, req.max_depth):
                    rows.append(edge_row("downstream", e))

        # Remove duplicates while preserving order
        seen = set()
        unique_rows = []
        for row in rows:
            row_tuple = tuple(row)
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)

        if not unique_rows:
            # Show info about the matched columns
            if len(targets) == 1:
                unique_rows = [[str(targets[0]), str(targets[0]), "info", "", "No relationships found"]]
            else:
                unique_rows = [[f"Matched {len(targets)} columns", "", "info", "", f"Pattern: {req.selector}"]]

        return {
            "columns": ["from", "to", "direction", "transformation", "description"],
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

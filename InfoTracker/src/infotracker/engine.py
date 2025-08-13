from __future__ import annotations

import json, yaml, fnmatch
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, List

from .config import RuntimeConfig
from .adapters import get_adapter
from .diff import BreakingChangeDetector, Change, Severity
from .models import ColumnGraph, ColumnNode, ObjectInfo, ColumnSchema, TableSchema


@dataclass
class ExtractRequest:
    sql_dir: Path
    out_dir: Path
    adapter: str
    catalog: Optional[Path] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    fail_on_warn: bool = False


@dataclass
class ImpactRequest:
    selector: str
    direction: str = "downstream"
    max_depth: Optional[int] = None


@dataclass
class DiffRequest:
    base: str
    head: str
    sql_dir: Path
    adapter: str
    severity_threshold: str = "BREAKING"


class Engine:
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self._column_graph: Optional[ColumnGraph] = None
    # engine.py (wewnątrz klasy Engine)
    def run_extract(self, req) -> Dict[str, Any]:
        """
        1) (opcjonalnie) Wczytaj catalog i zarejestruj w parser.schema_registry.
        2) Zbierz pliki wg include/exclude.
        3) Dla każdego pliku: parse -> adapter.extract_lineage (STR) -> zamień na dict -> zapisz JSON.
        4) Zwróć payload do _emit oraz policz warnings na bazie OL payloadu.
        """
        adapter = get_adapter(req.adapter)
        parser = adapter.parser

        warnings = 0

        # 1) Catalog
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
                                type=c.get("type"),
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
                    print(f"Warning: failed to load catalog from {catalog_path}: {e}")
            else:
                warnings += 1
                print(f"Warning: catalog path not found: {catalog_path}")

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

        # 3) Parsowanie i generacja OL
        out_dir = Path(req.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        outputs: List[List[str]] = []
        ignore_patterns: List[str] = list(getattr(self.config, "ignore", []) or [])

        for sql_path in sql_files:
            try:
                sql_text = sql_path.read_text(encoding="utf-8")

                # Parse do ObjectInfo (na potrzeby ignorów)
                obj_info: ObjectInfo = parser.parse_sql_file(sql_text, object_hint=sql_path.stem)
                obj_name = getattr(getattr(obj_info, "schema", None), "name", None) or getattr(obj_info, "name", None)
                if obj_name and ignore_patterns and any(fnmatch.fnmatch(obj_name, pat) for pat in ignore_patterns):
                    continue

                # Adapter zwraca STRING → normalizujemy do dict
                ol_raw = adapter.extract_lineage(sql_text, object_hint=sql_path.stem)
                ol_payload: Dict[str, Any] = json.loads(ol_raw) if isinstance(ol_raw, str) else ol_raw

                # Zapis do pliku
                target = out_dir / f"{sql_path.stem}.json"
                target.write_text(json.dumps(ol_payload, indent=2, ensure_ascii=False), encoding="utf-8")

                outputs.append([str(sql_path), str(target)])

                # warnings: unknown lub „pusty” payload
                has_schema_fields = bool(ol_payload.get("schema", {}).get("fields"))
                has_col_lineage  = bool(ol_payload.get("facets", {}).get("columnLineage", {}).get("fields"))
                if getattr(obj_info, "object_type", "unknown") == "unknown" or not (has_schema_fields or has_col_lineage):
                    warnings += 1

            except Exception as e:
                warnings += 1
                print(f"Warning: failed to process {sql_path}: {e}")

        return {
            "columns": ["input_sql", "openlineage_json"],
            "rows": outputs,
            "warnings": warnings,
        }


    def run_impact(self, req: ImpactRequest) -> Dict[str, Any]:
        """Run impact analysis for a column selector."""
        if not self._column_graph:
            return {
                "error": "No column graph available. Run 'extract' command first.",
                "selector": req.selector,
                "direction": req.direction,
                "results": [],
            }
        
        # Parse selector to determine direction and column
        selector = req.selector.strip()
        upstream_seed = selector.startswith('+')
        downstream_seed = selector.endswith('+')
        
        # Clean selector (remove + prefixes/suffixes)
        clean_selector = selector.strip('+')
        
        # Normalize selector: handle friendly db.schema.object.column format
        if clean_selector.count(".") == 3 and not clean_selector.startswith("mssql://"):
            db, schema, obj, col = clean_selector.split(".")
            # Use namespace from config or default
            ns = getattr(self.config, 'default_namespace', "mssql://localhost/InfoTrackerDW")
            clean_selector = f"{ns}.{schema}.{obj}.{col}"
        # Find the target column
        target_column = self._column_graph.find_column(clean_selector)
        if not target_column:
            return {
                "error": f"Column not found: {clean_selector}",
                "selector": req.selector,
                "direction": req.direction,
                "results": [],
            }
        
        # Determine direction based on selector format and explicit direction
        if upstream_seed and downstream_seed:
            # Both directions: +selector+
            direction = "both"
        elif upstream_seed:
            # Upstream only: +selector
            direction = "upstream"
        elif downstream_seed:
            # Downstream only: selector+
            direction = "downstream"
        else:
            # Use explicit direction parameter
            direction = req.direction.lower()
        
        results = []
        
        # Get upstream dependencies
        if direction in ["upstream", "both"]:
            upstream_edges = self._column_graph.get_upstream(target_column, req.max_depth)
            for edge in upstream_edges:
                results.append({
                    "column": str(edge.from_column),
                    "relationship": "upstream",
                    "transformation_type": edge.transformation_type.value,
                    "transformation_description": edge.transformation_description,
                    "path": f"{edge.from_column} -> {edge.to_column}"
                })
        
        # Get downstream dependencies
        if direction in ["downstream", "both"]:
            downstream_edges = self._column_graph.get_downstream(target_column, req.max_depth)
            for edge in downstream_edges:
                results.append({
                    "column": str(edge.to_column),
                    "relationship": "downstream",
                    "transformation_type": edge.transformation_type.value,
                    "transformation_description": edge.transformation_description,
                    "path": f"{edge.from_column} -> {edge.to_column}"
                })
        
        return {
            "selector": req.selector,
            "direction": direction,
            "target_column": str(target_column),
            "results": results,
            "total_dependencies": len(results)
        }

    def run_diff(self, req: DiffRequest) -> Dict[str, Any]:
        """Compare two versions and detect breaking changes."""
        from .adapters import get_adapter
        
        # Get base version lineage
        base_objects = self._extract_objects_for_diff(req.base, req.sql_dir, req.adapter)
        
        # Get head version lineage  
        head_objects = self._extract_objects_for_diff(req.head, req.sql_dir, req.adapter)
        
        # Detect changes
        detector = BreakingChangeDetector()
        changes = detector.detect_changes(base_objects, head_objects)
        
        # Filter by severity threshold
        threshold_severity = Severity(req.severity_threshold)
        severity_order = {
            Severity.NON_BREAKING: 0,
            Severity.POTENTIALLY_BREAKING: 1,
            Severity.BREAKING: 2
        }
        
        filtered_changes = [
            c for c in changes 
            if severity_order[c.severity] >= severity_order[threshold_severity]
        ]
        
        # Get summary
        summary = detector.get_summary()
        
        # Determine exit code: 0=no changes, 1=non-breaking changes, 2=breaking changes
        breaking_count = detector.get_breaking_count()
        non_breaking_count = len(changes) - breaking_count
        
        if breaking_count > 0:
            exit_code = 2  # Breaking changes detected
        elif non_breaking_count > 0:
            exit_code = 1  # Non-breaking changes detected
        else:
            exit_code = 0  # No changes
        
        return {
            "base": req.base,
            "head": req.head,
            "severity_threshold": req.severity_threshold,
            "total_changes": len(changes),
            "filtered_changes": len(filtered_changes),
            "breaking_changes": detector.get_breaking_count(),
            "changes": [detector._change_to_dict(c) for c in filtered_changes],
            "summary": summary,
            "exit_code": exit_code
        }
    
    def _extract_objects_for_diff(self, version: str, sql_dir: Path, adapter_name: str) -> List[ObjectInfo]:
        """
        Extract ObjectInfo list for given version.
        TODO: switch to real git checkout; for now read from working tree.
        """
        adapter = get_adapter(adapter_name)
        objects: List[ObjectInfo] = []

        for sql_file in sorted(sql_dir.rglob("*.sql")):
            try:
                sql = sql_file.read_text(encoding="utf-8")
                # ⬇️ używamy parsera, zwracamy ObjectInfo
                obj_info = adapter.parser.parse_sql_file(sql, object_hint=sql_file.stem)
                objects.append(obj_info)
            except Exception as e:
                print(f"Warning: Failed to process {sql_file}: {e}")
                continue

        return objects


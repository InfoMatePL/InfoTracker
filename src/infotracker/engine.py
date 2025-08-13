# src/infotracker/engine.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from fnmatch import fnmatch

import yaml

from .adapters import get_adapter
from .models import (
    ObjectInfo,
    ColumnSchema,
    TableSchema,
    ColumnGraph,
    ColumnNode,
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


@dataclass
class ImpactRequest:
    selector: str
    max_depth: int = 2
    graph_dir: Optional[Path] = None


@dataclass
class DiffRequest:
    sql_dir: Path
    adapter: str
    base: Path
    head: Optional[Path] = None
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
        adapter = get_adapter(req.adapter)
        parser = adapter.parser

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

        # 3) Parsowanie i generacja OL
        out_dir = Path(req.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        outputs: List[List[str]] = []
        parsed_objects: List[ObjectInfo] = []

        ignore_patterns: List[str] = list(getattr(self.config, "ignore", []) or [])

        for sql_path in sql_files:
            try:
                sql_text = sql_path.read_text(encoding="utf-8")

                # Parse do ObjectInfo (na potrzeby ignorów i grafu)
                obj_info: ObjectInfo = parser.parse_sql_file(sql_text, object_hint=sql_path.stem)
                parsed_objects.append(obj_info)

                # ignore po nazwie obiektu (string), nie po ObjectInfo
                obj_name = getattr(getattr(obj_info, "schema", None), "name", None) or getattr(obj_info, "name", None)
                if obj_name and ignore_patterns and any(fnmatch(obj_name, pat) for pat in ignore_patterns):
                    continue

                # Adapter → payload (str lub dict) → normalizacja do dict
                ol_raw = adapter.extract_lineage(sql_text, object_hint=sql_path.stem)
                ol_payload: Dict[str, Any] = json.loads(ol_raw) if isinstance(ol_raw, str) else ol_raw

                # Zapis do pliku (deterministyczny)
                target = out_dir / f"{sql_path.stem}.json"
                target.write_text(json.dumps(ol_payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

                outputs.append([str(sql_path), str(target)])

                # Heurystyka warnings – patrzymy w outputs[0].facets
                out0 = (ol_payload.get("outputs") or [])
                out0 = out0[0] if out0 else {}
                facets = out0.get("facets", {})
                has_schema_fields = bool(facets.get("schema", {}).get("fields"))
                has_col_lineage = bool(facets.get("columnLineage", {}).get("fields"))

                if getattr(obj_info, "object_type", "unknown") == "unknown" or not (has_schema_fields or has_col_lineage):
                    warnings += 1

            except Exception as e:
                warnings += 1
                logger.warning("failed to process %s: %s", sql_path, e)

        # 5) Budowa grafu kolumn z wszystkich sparsowanych obiektów
        # 5) Budowa grafu kolumn z wszystkich sparsowanych obiektów
        if parsed_objects:
            try:
                graph = ColumnGraph()
                graph.build_from_object_lineage(parsed_objects)  # ← użyj tej metody z models.py
                self._column_graph = graph

                # (opcjonalnie) zapisz graf na dysk, żeby impact mógł go wczytać w osobnym procesie
                graph_path = Path(req.out_dir) / "column_graph.json"
                edges_dump = []
                seen = set()
                for edges_list in graph._downstream_edges.values():  # prosty eksport krawędzi
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
            except Exception as e:
                logger.warning("failed to build column graph: %s", e)


        return {
            "columns": ["input_sql", "openlineage_json"],
            "rows": outputs,     # lista list – _emit to obsługuje
            "warnings": warnings,
        }

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
                    for edge in data.get("edges", []):
                        from_ns, from_tbl, from_col = edge["from"].split(".", 2)
                        to_ns, to_tbl, to_col = edge["to"].split(".", 2)
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
            # +column → downstream only
            direction_downstream = True
            sel = sel[1:]  # remove + from start
        elif sel.endswith('+'):
            # column+ → upstream only
            direction_upstream = True
            sel = sel[:-1]  # remove + from end
        else:
            # column → default (downstream)
            direction_downstream = True

        # Normalizacja selektora - obsługuj różne formaty:
        # 1. table.column -> dbo.table.column
        # 2. schema.table.column -> namespace/schema.table.column (jeśli nie ma protokołu)  
        # 3. pełny URI -> użyj jak jest
        if "://" in sel:
            # pełny URI, użyj jak jest
            pass
        else:
            parts = [p for p in sel.split(".") if p]
            if len(parts) == 2:
                # table.column -> dbo.table.column
                sel = f"dbo.{parts[0]}.{parts[1]}"
            elif len(parts) == 3:
                # schema.table.column -> namespace.schema.table.column  
                sel = f"mssql://localhost/InfoTrackerDW.{sel}"
            else:
                return {
                    "columns": ["message"],
                    "rows": [[f"Unsupported selector format: '{req.selector}'. Use 'table.column', 'schema.table.column', or full URI."]],
                }

        target = self._column_graph.find_column(sel)
        if not target:
            return {
                "columns": ["message"],
                "rows": [[f"Column '{sel}' not found in graph."]],
            }

        rows: List[List[str]] = []

        def edge_row(direction: str, e) -> List[str]:
            return [
                str(e.from_column), 
                str(e.to_column), 
                direction,
                getattr(e.transformation_type, "value", str(e.transformation_type)),
                e.transformation_description or "",
            ]

        if direction_upstream:
            for e in self._column_graph.get_upstream(target, req.max_depth):
                rows.append(edge_row("upstream", e))
        if direction_downstream:
            for e in self._column_graph.get_downstream(target, req.max_depth):
                rows.append(edge_row("downstream", e))

        return {
            "columns": ["from", "to", "direction", "transformation", "description"],
            "rows": rows or [[str(target), str(target), "info", "", "No relationships found"]],
        }


    # ------------------ DIFF (stub – jeśli masz swoją wersję, zostaw ją) ------------------

    def run_diff(self, req: DiffRequest) -> Dict[str, Any]:
        """
        Placeholder: jeśli masz pełną implementację porównywania, zostaw ją.
        Tu tylko zwracamy kod 0, żeby nie blokować CLI.
        """
        return {"columns": ["message"], "rows": [["Diff not implemented in this stub"]], "exit_code": 0}

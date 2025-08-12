from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

from .config import RuntimeConfig
from .adapters import get_adapter


@dataclass
class ExtractRequest:
    sql_dir: Path
    out_dir: Path
    adapter: str
    catalog: Optional[Path] = None
    include: Optional[str] = None
    exclude: Optional[str] = None
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

    def run_extract(self, req: ExtractRequest) -> Dict[str, Any]:
        adapter = get_adapter(req.adapter)
        req.out_dir.mkdir(parents=True, exist_ok=True)

        sql_files = [
            p for p in sorted(req.sql_dir.glob("**/*.sql"))
            if (not req.include or p.match(req.include)) and (not req.exclude or not p.match(req.exclude))
        ]
        outputs = []
        for sql_file in sql_files:
            sql_text = sql_file.read_text()
            obj_name = sql_file.stem
            lineage = adapter.extract_lineage(sql_text, object_hint=obj_name)
            out_path = req.out_dir / f"{sql_file.stem}.json"
            out_path.write_text(lineage)
            outputs.append({"input": str(sql_file), "output": str(out_path)})
        return {"columns": ["input", "output"], "rows": outputs}

    def run_impact(self, req: ImpactRequest) -> Dict[str, Any]:
        # Placeholder structure; to be implemented
        return {
            "selector": req.selector,
            "direction": req.direction,
            "results": [],
        }

    def run_diff(self, req: DiffRequest) -> Dict[str, Any]:
        # Placeholder: return no changes with exit_code 0
        return {"changes": [], "exit_code": 0}


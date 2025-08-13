from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class RuntimeConfig:
    default_adapter: str = "mssql"
    default_database: Optional[str] = None
    sql_dir: str = "examples/warehouse/sql"
    out_dir: str = "build/lineage"
    include: List[str] = field(default_factory=lambda: ["*.sql"])
    exclude: List[str] = field(default_factory=list)
    severity_threshold: str = "BREAKING"
    ignore: List[str] = field(default_factory=list)
    catalog: Optional[str] = None
    log_level: str = "info"
    output_format: str = "text"


def load_config(path: Optional[Path]) -> RuntimeConfig:
    cfg = RuntimeConfig()
    if path is None:
        # Try repo root default
        default = Path("infotracker.yml")
        if default.exists():
            path = default
    if path and path.exists():
        data = yaml.safe_load(path.read_text()) or {}
        for k, v in data.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
    
    # Load .infotrackerignore if exists
    ignore_file = Path(".infotrackerignore")
    if ignore_file.exists():
        try:
            ignore_patterns = []
            for line in ignore_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    ignore_patterns.append(line)
            cfg.ignore.extend(ignore_patterns)
        except Exception as e:
            print(f"Warning: failed to load .infotrackerignore: {e}")
    
    return cfg


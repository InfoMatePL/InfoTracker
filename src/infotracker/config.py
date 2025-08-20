from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class OpenLineageCfg:
    namespace: str = "mssql"


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
    openlineage: OpenLineageCfg = field(default_factory=OpenLineageCfg)


def load_config(path: Optional[Path]) -> RuntimeConfig:
    cfg = RuntimeConfig()
    if path is None:
        # Try package default first (works for installed packages)
        try:
            import importlib.resources
            from . import __package__
            
            if hasattr(importlib.resources, 'files'):  # Python 3.9+
                package_config = importlib.resources.files('infotracker') / 'infotracker.yml'
                if package_config.is_file():
                    path = Path(str(package_config))
            else:  # Python 3.8 fallback
                with importlib.resources.path('infotracker', 'infotracker.yml') as p:
                    if p.exists():
                        path = p
        except (ImportError, AttributeError):
            pass
        
        # Fallback to current directory (development mode)
        if path is None:
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
    patterns: list[str] = []

    if ignore_file.exists():
        try:
            for line in ignore_file.read_text(encoding="utf-8").splitlines():
                # utnij komentarz inline i białe znaki
                line = line.split("#", 1)[0].strip()
                if line:
                    patterns.append(line)
        except Exception as e:
            print(f"Warning: failed to load .infotrackerignore: {e}")

    # scal z configiem
    base = list(getattr(cfg, "ignore", []) or [])
    cfg.ignore = sorted(set(base + patterns))
    
    return cfg


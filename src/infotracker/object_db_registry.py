from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict

Key = str  # "<type>::<schema>.<object>" or "*::<schema>.<object>"


def _key(obj_type: str, schema_table: str) -> Key:
    return f"{obj_type.lower()}::{schema_table.lower()}"


def _wild(schema_table: str) -> Key:
    return f"*::{schema_table.lower()}"


@dataclass
class ObjectDbRegistry:
    hard: Dict[Key, str] = field(default_factory=dict)
    soft: Dict[Key, Counter] = field(default_factory=lambda: defaultdict(Counter))
    path: Optional[Path] = None

    @classmethod
    def load(cls, path: str | Path) -> "ObjectDbRegistry":
        p = Path(path)
        if not p.exists():
            return cls(path=p)
        data = json.loads(p.read_text(encoding="utf-8"))
        reg = cls(path=p)
        reg.hard = {k: v for k, v in data.get("hard", {}).items()}
        for k, d in data.get("soft", {}).items():
            reg.soft[k] = Counter(d)
        return reg

    def save(self, path: Optional[str | Path] = None) -> None:
        p = Path(path) if path else (self.path or Path("build/object_db_map.json"))
        p.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "hard": self.hard,
            "soft": {k: dict(c) for k, c in self.soft.items()},
        }
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        self.path = p

    # ---- learning API ----
    def learn_from_create(self, obj_type: str, schema_table: str, db: str) -> None:
        if not (schema_table and db):
            return
        self.hard[_key(obj_type, schema_table)] = db

    def learn_from_targets(self, schema_table: str, db: str) -> None:
        if not (schema_table and db):
            return
        self.hard[_wild(schema_table)] = db
        self.soft[_wild(schema_table)][db] += 10

    def learn_from_references(self, schema_table: str, db: str) -> None:
        if not (schema_table and db):
            return
        self.soft[_wild(schema_table)][db] += 1

    # ---- resolution API ----
    def get(self, key_or_type: str, schema_table: Optional[str] = None) -> Optional[str]:
        if schema_table is None:
            return self.hard.get(key_or_type)
        k1 = _key(key_or_type, schema_table)
        k2 = _wild(schema_table)
        return self.hard.get(k1) or self.hard.get(k2)

    def resolve(self, obj_type: str, schema_table: str, fallback: Optional[str] = None) -> str:
        k1 = _key(obj_type, schema_table)
        if k1 in self.hard:
            return self.hard[k1]
        k2 = _wild(schema_table)
        if k2 in self.hard:
            return self.hard[k2]
        c = self.soft.get(k1) or self.soft.get(k2)
        if c:
            top = c.most_common(2)
            if len(top) == 1 or (len(top) > 1 and top[0][1] > top[1][1]):
                return top[0][0]
        return fallback or "InfoTrackerDW"


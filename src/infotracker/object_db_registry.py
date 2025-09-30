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
        """Resolve DB for given object type and schema.table.

        Resolution precedence (stable, deterministic):
        1) If a specific hard mapping exists and it's not a weak default, return it.
        2) Otherwise, if a wildcard hard mapping exists, prefer it when it differs from a weak default.
        3) Otherwise, if soft votes exist (specific or wildcard) with a clear leader, return the leader.
        4) Fallback to provided default or "InfoTrackerDW".

        Rationale: Avoid fragmenting namespaces when an early hard mapping points to a generic
        default DB like "InfoTrackerDW". Prefer later-learned wildcard/soft signals that
        consistently map an object to a real DB (e.g., STG, EDW_CORE).
        """
        weak_defaults = {"infotrackerdb", "infotrackerdw"}

        k1 = _key(obj_type, schema_table)
        k2 = _wild(schema_table)

        specific_hard = self.hard.get(k1)
        wildcard_hard = self.hard.get(k2)

        # 1) Strong specific hard mapping
        if specific_hard and str(specific_hard).lower() not in weak_defaults:
            return specific_hard

        # 2) Prefer wildcard hard over weak specific default
        if wildcard_hard:
            # If specific is weak or absent, or differs from wildcard, choose wildcard
            if (not specific_hard) or (str(specific_hard).lower() in weak_defaults) or (wildcard_hard != specific_hard):
                return wildcard_hard

        # 3) Soft votes (specific first, then wildcard)
        c = self.soft.get(k1) or self.soft.get(k2)
        if c:
            top = c.most_common(2)
            if len(top) == 1 or (len(top) > 1 and top[0][1] > top[1][1]):
                candidate = top[0][0]
                # If specific is weak default or absent, allow soft leader
                if (not specific_hard) or (str(specific_hard).lower() in weak_defaults):
                    return candidate

        # 4) Fallbacks
        if specific_hard:
            return specific_hard
        return fallback or "InfoTrackerDW"

    def promote_soft(
        self,
        min_votes: int = 2,
        min_margin: int = 1,
        override_weak_hard: bool = True,
        weak_defaults: tuple[str, ...] = ("infotrackerdb",),
    ) -> int:
        """
        Promote soft votes to hard mappings when a clear majority exists.
        If override_weak_hard is True, allow overriding existing hard entries when
        the current hard DB is in weak_defaults (treated as weak/default DB).

        Returns the count of added/overridden mappings.
        """
        added = 0
        weak_set = {w.lower() for w in weak_defaults}
        for key, counter in list(self.soft.items()):
            mc = counter.most_common(2)
            if not mc:
                continue
            (top_db, top_cnt) = mc[0]
            sec_cnt = mc[1][1] if len(mc) > 1 else 0
            if top_cnt >= min_votes and (top_cnt - sec_cnt) >= min_margin:
                if key in self.hard:
                    hard_db = str(self.hard[key]).lower()
                    if override_weak_hard and hard_db in weak_set:
                        self.hard[key] = top_db
                        added += 1
                else:
                    self.hard[key] = top_db
                    added += 1
        return added

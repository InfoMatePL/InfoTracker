from __future__ import annotations
from typing import List, Optional, Set, Tuple

from ..models import ColumnLineage, ColumnReference, TransformationType


# ---- Helpers: procedure accumulator ----
def _proc_acc_init(self, target_fqn: str) -> None:
    self._proc_acc.setdefault(target_fqn, {})


def _proc_acc_add(self, target_fqn: str, col_lineage: List[ColumnLineage]) -> None:
    acc = self._proc_acc.setdefault(target_fqn, {})
    for lin in (col_lineage or []):
        s = acc.setdefault(lin.output_column, set())
        for ref in (lin.input_fields or []):
            try:
                s.add((ref.namespace, ref.table_name, ref.column_name))
            except Exception:
                s.add((str(getattr(ref, "namespace", "")), str(getattr(ref, "table_name", "")), str(getattr(ref, "column_name", ""))))


def _proc_acc_finalize(self, target_fqn: str) -> List[ColumnLineage]:
    acc = self._proc_acc.get(target_fqn, {})
    out: List[ColumnLineage] = []
    for col, inputs in acc.items():
        refs = [ColumnReference(namespace=a, table_name=b, column_name=c) for (a, b, c) in sorted(inputs)]
        out.append(ColumnLineage(
            output_column=col,
            input_fields=refs,
            transformation_type=TransformationType.IDENTITY,
            transformation_description="merged from multiple branches"
        ))
    return out


# ---- Helpers: temp versioning ----
def _temp_next(self, name: str) -> str:
    v = self._temp_version.get(name, 0) + 1
    self._temp_version[name] = v
    return f"{name}@{v}"


def _temp_current(self, name: str) -> Optional[str]:
    v = self._temp_version.get(name)
    return f"{name}@{v}" if v else None


def _canonical_temp_name(self, name: str) -> str:
    """Return a stable canonical name for a temp table, preferring the current version if available."""
    try:
        n = name if name.startswith('#') else f"#{name}"
        if '@' in n:
            return n
        cur = _temp_current(self, n)
        return cur or n
    except Exception:
        return name

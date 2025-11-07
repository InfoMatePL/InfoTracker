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
    """Return canonical temp name including context when available.

    - If we know current object context, format: DB.schema.object.#temp[@v]
    - Otherwise, return '#temp[@v]' to avoid breaking callers.
    """
    try:
        n = name if name.startswith('#') else f"#{name}"
        ver = _temp_current(self, n)
        seg = ver or n
        ctx_db = getattr(self, "_ctx_db", None) or getattr(self, "current_database", None) or getattr(self, "default_database", None)
        ctx_obj = getattr(self, "_ctx_obj", None)
        if ctx_db and ctx_obj:
            return f"{ctx_db}.{ctx_obj}.{seg}"
        return seg
    except Exception:
        return name


def _extract_temp_name(self, raw_name: str) -> str:
    """Extract clean temp table name from a raw identifier string.

    Handles cases like:
    - '#tmp' → 'tmp'
    - 'dbo.#tmp' → 'tmp'
    - '#tmp INTO' or '#tmp (' → 'tmp'
    - '#tmp_COALESCE(x,y)' → 'tmp'

    Returns only valid identifier characters (alphanumeric + underscore)
    immediately following the last '#'. If input doesn't contain '#',
    returns it unchanged.
    """
    try:
        if not raw_name or '#' not in str(raw_name):
            return raw_name
        text = str(raw_name)
        # Get part after last '#'
        after_hash = text.split('#')[-1]
        # Extract only valid identifier chars (stop at first non-identifier char)
        import re as _re
        m = _re.match(r"([A-Za-z0-9_]+)", after_hash)
        return m.group(1) if m else after_hash
    except Exception:
        return raw_name

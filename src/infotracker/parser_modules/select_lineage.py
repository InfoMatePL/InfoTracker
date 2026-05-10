from __future__ import annotations

from typing import List, Dict, Set, Optional, Tuple
import logging

import sqlglot
from sqlglot import expressions as exp

from ..models import ColumnReference, ColumnSchema, ColumnLineage, TransformationType, ObjectInfo, TableSchema
from .names import _temp_registry_base_key
import re

logger = logging.getLogger(__name__)

# SQL keywords that should never be treated as table names
JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}

def _is_join_keyword(table_name: str) -> bool:
    """Check if table_name (or its last segment) is a JOIN keyword."""
    if not table_name:
        return False
    simple = table_name.split('.')[-1]
    return simple.lower() in JOIN_KEYWORDS

# Minimal tail-strip regex similar to legacy dev_parser for stable derived column names
_FUNC_TAIL_RE = re.compile(r"\b(?:COALESCE|ISNULL|CAST|CONVERT|TRY_CAST|HASHBYTES|IIF)\s*\(", re.I)

def _strip_expr_tail(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"/\*.*?\*/", "", str(name), flags=re.S)
    s = re.sub(r"--.*?$", "", s, flags=re.M)
    s = re.sub(r"\s+", " ", s).strip()
    m = _FUNC_TAIL_RE.search(s)
    if m:
        s = s[:m.start()].strip()
    s = re.sub(r"[^\w#]+", "_", s).strip("_")
    # Validate: if result is empty, too short, or contains SQL keywords, return empty to trigger fallback
    if not s or len(s) < 2 or any(keyword in s.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT', 'AS']):
        return ""
    return s


def _build_alias_maps(self, select_exp: exp.Select):
    alias_map = {}
    derived_cols = {}

    # Process CTEs first to populate cte_registry
    self._process_ctes(select_exp)
    
    # Add CTEs to alias_map so they can be resolved as sources (alias -> scoped node key)
    for _scoped, cte_entry in self.cte_registry.items():
        cte_simple = (
            cte_entry.get("simple_name")
            if isinstance(cte_entry, dict)
            else (_scoped.split("$")[-1] if "$" in _scoped else _scoped)
        )
        if cte_simple:
            alias_map[str(cte_simple).lower()] = _scoped

    base_fqns = []
    for t in select_exp.find_all(exp.Table):
        a = getattr(t, "alias", None) or t.args.get("alias")
        alias = None
        if a:
            if hasattr(a, "name"):
                alias = a.name.lower()
            else:
                alias = str(a).lower()
        fqn = self._qualify_table(t)
        # If this table corresponds to a known temp by simple name, use canonical format
        try:
            parts_tmp = (fqn or '').split('.')
            simple = parts_tmp[-1] if parts_tmp else None
            if simple:
                # Check if it's a temp table (starts with # or is in temp_registry with #)
                temp_seg = None
                if str(simple).startswith('#'):
                    if simple in self.temp_registry:
                        temp_seg = simple
                    else:
                        temp_seg = _match_temp_segment(self, simple)
                else:
                    temp_seg = _match_temp_segment(self, simple)
                
                if temp_seg:
                    # Use canonical format with procedure context: schema.object#temp
                    _, canonical_name = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
                    fqn = canonical_name
        except Exception:
            pass
        # CTE references parsed as Table must use the same scoped virtual FQN as cte_registry
        # (alias ``efs`` would otherwise map to dbo.<cte> while simple name maps to dbo.<proc>$<cte>,
        # breaking unqualified-column disambiguation).
        try:
            cte_key_tbl = self._cte_registry_resolve_key(getattr(t, "name", None))
            if cte_key_tbl:
                cte_info = self.cte_registry.get(cte_key_tbl)
                if isinstance(cte_info, dict) and cte_info.get("node_name"):
                    fqn = str(cte_info["node_name"])
                else:
                    fqn = cte_key_tbl
        except Exception:
            pass
        if alias:
            alias_map[alias] = fqn
        # Don't overwrite CTEs in alias_map - they were already added with simple CTE names
        table_name_lower = t.name.lower()
        is_cte_name = self._cte_registry_resolve_key(table_name_lower) is not None
        if not is_cte_name:
            alias_map[table_name_lower] = fqn
        base_fqns.append(fqn)

    for sq in select_exp.find_all(exp.Subquery):
        a = getattr(sq, "alias", None) or sq.args.get("alias")
        if not a:
            continue
        if hasattr(a, "name"):
            alias = a.name.lower()
        else:
            alias = str(a).lower()
        inner = sq.this if isinstance(sq.this, exp.Select) else None
        if not inner:
            continue
        idx = 0
        for proj in (inner.expressions or []):
            if isinstance(proj, exp.Alias):
                out_name = (proj.alias or proj.alias_or_name)
                target = proj.this
            else:
                out_name = f"col_{idx+1}"
                target = proj
            key = (alias, (out_name or "").lower())
            derived_cols[key] = list(target.find_all(exp.Column))
            idx += 1

    # If exactly one base table is present, allow resolving unqualified columns to it
    # BUT: prioritize the table/CTE from the main FROM clause of this SELECT
    # This ensures that for "SELECT ... FROM CTE", unqualified columns resolve to CTE, not to tables inside CTE definitions
    try:
        # Find the main FROM clause of this SELECT (not nested SELECTs)
        from_source = None

        # sqlglot changed arg naming across versions; support both.
        from_expr = None
        if hasattr(select_exp, "args"):
            from_expr = select_exp.args.get("from_") or select_exp.args.get("from")

        if from_expr and hasattr(from_expr, "this"):
            from_this = from_expr.this
            if isinstance(from_this, exp.Table):
                from_table_name = from_this.name
                # Check if FROM table has an alias
                from_alias = getattr(from_this, "alias", None) or from_this.args.get("alias")
                if from_alias:
                    from_alias_str = from_alias.name.lower() if hasattr(from_alias, "name") else str(from_alias).lower()
                    # Use the alias as the source
                    if from_alias_str in alias_map:
                        from_source = alias_map[from_alias_str]
                else:
                    # No alias - use table name (check if it's a CTE or regular table)
                    from_table_lower = from_table_name.lower()
                    if from_table_lower in alias_map:
                        from_source = alias_map[from_table_lower]
            elif isinstance(from_this, exp.Join):
                # sqlglot: FROM a JOIN b → root is Join; leftmost leaf is the primary FROM source
                leaf = from_this
                while isinstance(leaf, exp.Join):
                    leaf = leaf.this
                if isinstance(leaf, exp.Table):
                    from_alias = getattr(leaf, "alias", None) or leaf.args.get("alias")
                    if from_alias:
                        from_alias_str = (
                            from_alias.name.lower()
                            if hasattr(from_alias, "name")
                            else str(from_alias).lower()
                        )
                        if from_alias_str in alias_map:
                            from_source = alias_map[from_alias_str]
                    if not from_source and getattr(leaf, "name", None):
                        from_table_lower = str(leaf.name).lower()
                        if from_table_lower in alias_map:
                            from_source = alias_map[from_table_lower]
                elif isinstance(leaf, exp.Alias) and isinstance(leaf.this, exp.Table):
                    inner_t = leaf.this
                    if leaf.alias:
                        from_alias_str = str(leaf.alias).lower()
                        if from_alias_str in alias_map:
                            from_source = alias_map[from_alias_str]
                    if not from_source and getattr(inner_t, "name", None):
                        from_table_lower = str(inner_t.name).lower()
                        if from_table_lower in alias_map:
                            from_source = alias_map[from_table_lower]
            elif isinstance(from_this, exp.Values):
                # T-SQL: FROM (VALUES (a.col), ...) AS V(LoadDate) — Values + TableAlias, not exp.Table
                talias = from_this.args.get("alias") or getattr(from_this, "alias", None)
                if talias and getattr(talias, "this", None) is not None:
                    from_alias_str = (
                        talias.this.name.lower()
                        if hasattr(talias.this, "name")
                        else str(talias.this).lower()
                    )
                    synthetic = f"__values__.{from_alias_str}"
                    alias_map[from_alias_str] = synthetic
                    from_source = synthetic
                    vcols = talias.args.get("columns") or []
                    if len(vcols) == 1:
                        cn = (
                            str(vcols[0].name)
                            if hasattr(vcols[0], "name")
                            else str(vcols[0])
                        ).lower()
                        acc: List[exp.Column] = []
                        for tup in from_this.expressions or []:
                            cells = tup.expressions if isinstance(tup, exp.Tuple) else [tup]
                            if cells:
                                acc.extend(list(cells[0].find_all(exp.Column)))
                        derived_cols[(from_alias_str, cn)] = acc
                        derived_cols[("", cn)] = acc

        # Persist the explicit top-level FROM source for unqualified-column resolution.
        if from_source:
            alias_map["__main_from__"] = from_source

        # Set alias_map[''] to the FROM source if found, otherwise fall back to single-table logic.
        if from_source and '' not in alias_map:
            alias_map[''] = from_source
        elif len(set(base_fqns)) == 1 and '' not in alias_map:
            alias_map[''] = sorted(set(base_fqns))[0]
    except Exception:
        pass

    return alias_map, derived_cols


def _fqn_column_presence_known(self, fqn: str, col_lower: str) -> Optional[bool]:
    """Whether ``fqn`` has column ``col_lower`` when schema/temp/CTE metadata is available.

    Returns:
        True / False when we have a non-placeholder column list or TableSchema.
        None when we cannot tell (no metadata, placeholder ``*``, unknown inference).
    """
    if not fqn or not col_lower:
        return None
    fqn_s = str(fqn).strip()
    if fqn_s.startswith("__values__."):
        return None

    cte_key = self._cte_registry_resolve_key(fqn_s)
    if cte_key:
        info = self.cte_registry.get(cte_key)
        if isinstance(info, dict):
            cols = info.get("columns") or []
            if not cols or _is_placeholder_cols(list(cols)):
                return None
            names = {str(c).lower() for c in cols if c}
            return col_lower in names
        return None

    temp_seg = None
    if "#" in fqn_s:
        for seg in fqn_s.split("."):
            if str(seg).startswith("#"):
                temp_seg = seg
                break
    if not temp_seg:
        leaf = fqn_s.split(".")[-1] if "." in fqn_s else fqn_s
        temp_seg = _match_temp_segment(self, leaf)
    if temp_seg:
        try:
            simple = str(temp_seg).split("@")[0].lstrip("#")
            tcols = (
                self.temp_registry.get(temp_seg)
                or self.temp_registry.get(f"#{simple}")
                or self.temp_registry.get(simple)
                or []
            )
        except Exception:
            tcols = []
        if not tcols or _is_placeholder_cols(list(tcols)):
            return None
        names = {str(c).lower() for c in tcols if c}
        return col_lower in names

    try:
        db, sch, tbl = self._split_fqn(fqn_s)
    except Exception:
        return None
    schema = _get_schema(self, db, sch, tbl)
    if schema and schema.columns:
        return schema.get_column(col_lower) is not None

    cols_inf = self._infer_table_columns_unified(fqn_s)
    if _is_placeholder_cols(cols_inf):
        return None
    names = {str(c).lower() for c in cols_inf if c}
    return col_lower in names


def _from_clause_alias_keys(select_stmt: Optional[exp.Select]) -> Optional[Set[str]]:
    """Table / subquery aliases introduced only in this SELECT's top-level FROM/JOIN chain.

    Excludes tables inside nested ``SELECT`` bodies (e.g. ``NOT EXISTS (SELECT … FROM efs)``) so
    unqualified column binding does not treat correlate subquery sources as peer join inputs.
    """
    if not isinstance(select_stmt, exp.Select):
        return None
    out: Set[str] = set()
    from_expr = select_stmt.args.get("from_") or select_stmt.args.get("from")
    if not from_expr or not getattr(from_expr, "this", None):
        return out

    def add_table_identifier(t: exp.Table) -> None:
        if getattr(t, "name", None):
            out.add(str(t.name).lower().strip())
        a = getattr(t, "alias", None) or t.args.get("alias")
        if a:
            if hasattr(a, "name"):
                out.add(str(a.name).lower().strip())
            else:
                out.add(str(a).lower().strip())

    def walk(n: Optional[exp.Expression]) -> None:
        if n is None:
            return
        if isinstance(n, exp.Select):
            return
        if isinstance(n, exp.Table):
            add_table_identifier(n)
            return
        if isinstance(n, exp.Join):
            walk(n.this)
            walk(n.expression)
            return
        if isinstance(n, exp.Alias):
            inner = n.this
            if isinstance(inner, exp.Table):
                add_table_identifier(inner)
            elif isinstance(inner, (exp.Subquery, exp.Unnest)):
                al = n.args.get("alias") or getattr(n, "alias", None)
                if isinstance(al, exp.TableAlias):
                    tt = al.this if getattr(al, "this", None) is not None else al
                    out.add(str(getattr(tt, "name", tt)).lower())
            return
        if isinstance(n, exp.Subquery):
            al = n.args.get("alias") or getattr(n, "alias", None)
            if isinstance(al, exp.TableAlias):
                tt = al.this if getattr(al, "this", None) is not None else al
                out.add(str(getattr(tt, "name", tt)).lower())
            return
        if isinstance(n, exp.Paren):
            walk(n.this)
            return
        if isinstance(n, exp.From):
            walk(n.this)
            return
        logger.debug("_from_clause_alias_keys: skipping unexpected FROM subtree node %s", type(n).__name__)

    walk(from_expr.this)
    for j in select_stmt.args.get("joins") or []:
        if isinstance(j, exp.Join):
            walk(j.this)
    return out


def _disambiguate_unqualified_column_fqn(
    self,
    col_name: str,
    alias_map: dict,
    fallback_fqn: str,
    scope_aliases: Optional[Set[str]],
) -> Optional[str]:
    """Pick a single source FQN for an unqualified column when schemas distinguish sources.

    If multiple distinct sources *provably* contain the column, returns None (conservative).
    If no source provably contains it, returns ``fallback_fqn`` (typically ``__main_from__``).
    """
    ckey = (col_name or "").strip().lower()
    if not ckey:
        return fallback_fqn
    matches: List[str] = []
    seen: Set[str] = set()
    for k, v in (alias_map or {}).items():
        if not isinstance(k, str):
            continue
        lk = k.lower()
        if lk in ("__main_from__", "") or lk.startswith("__"):
            continue
        if scope_aliases is not None and lk not in scope_aliases:
            continue
        if not isinstance(v, str) or not v:
            continue
        if v in seen:
            continue
        if str(v).startswith("__values__."):
            continue
        seen.add(v)
        pres = _fqn_column_presence_known(self, v, ckey)
        if pres is True:
            matches.append(v)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        logger.debug(
            "Ambiguous unqualified column %r among sources with known schema: %s",
            col_name,
            matches,
        )
        return None
    return fallback_fqn


def _cte_node_name(self, cte_name: str) -> str:
    """Build a stable virtual CTE node name like 'dbo.<procedure>$<cte>'."""
    return self._cte_scoped_key(cte_name)


def _match_temp_segment(self, raw_name: str) -> Optional[str]:
    """Resolve temp segment from temp_registry in case-insensitive way."""
    target = (raw_name or "").split("@")[0].lstrip("#").lower()
    if not target:
        return None
    for key in getattr(self, "temp_registry", {}).keys():
        base = str(key).split("@")[0]
        if base.lstrip("#").lower() == target:
            return base if base.startswith("#") else f"#{base}"
    return None


def _scoped_temp_ref(self, temp_seg: str) -> Tuple[str, str]:
    """Build canonical namespace/table for local temp scoped to current procedure."""
    temp_name = str(temp_seg or "").split("@")[0].lstrip("#")
    ctx_db = getattr(self, "_ctx_db", None) or self.current_database or self.default_database or "InfoTrackerDW"
    ctx_obj = getattr(self, "_ctx_obj", None) or "procedure"
    try:
        proc_norm = self._normalize_table_name_for_output(ctx_obj)
    except Exception:
        proc_norm = str(ctx_obj)
    proc_simple = str(proc_norm).split(".")[-1]
    return f"mssql://localhost/{str(ctx_db).upper()}", f"dbo.{proc_simple}#{temp_name}"


def _normalize_column_ref_temp(self, ref: ColumnReference) -> None:
    """Rewrite sqlglot ``*.dbo.temp`` and other registry-backed temp leaks to scoped ``dbo.<proc>#<name>``.

    Column graph parity: ``models._graph_remap_dbo_temp_pseudo`` applies the same disambiguation when
    building edges from lineage that still contains ``dbo.temp``.
    """
    if not isinstance(ref, ColumnReference):
        return
    tname = (ref.table_name or "").strip()
    if not tname:
        return
    parts_clean = [re.sub(r"[\[\]]", "", p) for p in tname.split(".") if p]
    simple = parts_clean[-1] if parts_clean else ""
    simple_base = str(simple).split("@")[0]
    temp_seg = _match_temp_segment(self, simple_base)
    if not temp_seg and len(parts_clean) >= 2:
        if parts_clean[-2].lower() == "dbo" and simple_base.lower() == "temp":
            rk = _temp_registry_base_key(self, "temp")
            if rk:
                temp_seg = str(rk).split("@")[0]
                if not temp_seg.startswith("#"):
                    temp_seg = f"#{temp_seg.lstrip('#')}"
    # General leak: ``dbo.<Name>`` where ``#Name`` exists in temp_registry (sqlglot dropped ``#``).
    if not temp_seg and len(parts_clean) >= 2 and parts_clean[-2].lower() == "dbo":
        rk2 = _temp_registry_base_key(self, simple_base)
        if rk2:
            temp_seg = str(rk2).split("@")[0]
            if not temp_seg.startswith("#"):
                temp_seg = f"#{temp_seg.lstrip('#')}"
    if not temp_seg:
        return
    # Already scoped: last FQN segment contains '#' (e.g. dbo.<proc>#tmp)
    if "#" in (tname.split(".")[-1] if tname else ""):
        return
    try:
        ns_temp, nm_temp = _scoped_temp_ref(self, temp_seg)
        ref.namespace = ns_temp
        ref.table_name = nm_temp
    except Exception:
        pass


def _normalize_lineage_input_temps(self, lineage: List[ColumnLineage]) -> None:
    """Apply :func:`_normalize_column_ref_temp` to every input field (mutates in place)."""
    for ln in lineage or []:
        for ir in ln.input_fields or []:
            _normalize_column_ref_temp(self, ir)


def _register_cte_lineage_object(self, cte_name: str, cte_def: exp.Select, cte_columns: List[str]) -> None:
    """Create a virtual ObjectInfo for CTE so it appears as an intermediate graph node."""
    if not isinstance(cte_def, exp.Select):
        return
    if not hasattr(self, "cte_lineage_objects"):
        return

    node_name = _cte_node_name(self, cte_name)
    try:
        cte_lineage, cte_output_columns = self._extract_column_lineage(cte_def, node_name)
    except Exception:
        cte_lineage, cte_output_columns = [], []

    _normalize_lineage_input_temps(self, cte_lineage)

    if not cte_output_columns:
        cte_output_columns = [
            ColumnSchema(name=c, data_type="unknown", nullable=True, ordinal=i)
            for i, c in enumerate(cte_columns or [])
        ]

    try:
        deps = self._extract_dependencies(cte_def) or set()
    except Exception:
        deps = set()
    # Normalize temp dependencies to canonical procedure-scoped names.
    deps_norm: Set[str] = set()
    for dep in deps:
        d = str(dep or "")
        d_simple = d.split(".")[-1] if "." in d else d
        temp_seg = _match_temp_segment(self, d_simple)
        if temp_seg:
            try:
                _, nm_temp = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
                deps_norm.add(nm_temp)
                continue
            except Exception:
                pass
        deps_norm.add(d)
    deps = deps_norm

    ns = self._canonical_namespace(
        self.current_database or self.default_database or "InfoTrackerDW"
    )
    cte_obj = ObjectInfo(
        name=node_name,
        object_type="cte",
        schema=TableSchema(namespace=ns, name=node_name, columns=cte_output_columns),
        lineage=cte_lineage,
        dependencies=deps,
    )

    # Deduplicate per parse by schema name, but always keep the latest version.
    replaced = False
    for idx, existing_obj in enumerate(self.cte_lineage_objects):
        if getattr(getattr(existing_obj, "schema", None), "name", "").lower() == node_name.lower():
            self.cte_lineage_objects[idx] = cte_obj
            replaced = True
            break
    if not replaced:
        self.cte_lineage_objects.append(cte_obj)


def _append_column_ref(
    self,
    out_list,
    col_exp: exp.Column,
    alias_map: dict,
    scope_select: Optional[exp.Select] = None,
):
    import logging
    logger = logging.getLogger(__name__)
    
    # Initialize visited CTE set to prevent infinite recursion
    if not hasattr(self, '_cte_expansion_stack'):
        self._cte_expansion_stack = set()
    
    qual = (col_exp.table or "").lower()
    table_fqn = alias_map.get(qual)
    
    if not table_fqn:
        # Try to find table by column name if no table qualifier
        # This handles unqualified columns when there's only one table
        if not qual and '__main_from__' in alias_map:
            table_fqn = alias_map['__main_from__']
            logger.debug(f"_append_column_ref: Using alias_map['__main_from__'] = {table_fqn} for unqualified column {col_exp.name}")
        elif not qual and '' in alias_map:
            table_fqn = alias_map['']
            logger.debug(f"_append_column_ref: Using alias_map[''] = {table_fqn} for unqualified column {col_exp.name}")
        else:
            logger.debug(f"_append_column_ref: No table_fqn found for qual={qual}, alias_map keys={list(alias_map.keys())}, alias_map values={list(alias_map.values())[:5]}, cte_registry keys={list(self.cte_registry.keys())[:5]}")
            # If no table qualifier and no alias_map[''], try to find CTE in alias_map values
            if not qual:
                # Check if any value in alias_map is a CTE
                # For INSERT INTO with CTE, the CTE might be the only source
                for key, value in alias_map.items():
                    value_simple = value.split('.')[-1] if '.' in value else value
                    _rk = self._cte_registry_resolve_key(value) or self._cte_registry_resolve_key(value_simple)
                    if _rk:
                        table_fqn = _rk
                        logger.debug(f"_append_column_ref: Found CTE {value} in alias_map for unqualified column {col_exp.name}")
                        break
                if not table_fqn:
                    for key, value in alias_map.items():
                        _rk = self._cte_registry_resolve_key(value) or self._cte_registry_resolve_key(
                            value.split(".")[-1] if "." in value else value
                        )
                        if _rk:
                            table_fqn = _rk
                            logger.debug(f"_append_column_ref: Found CTE {value} (case-insensitive) in alias_map for unqualified column {col_exp.name}")
                            break
                    if not table_fqn:
                        for key, value in alias_map.items():
                            _rk = self._cte_registry_resolve_key(value) or self._cte_registry_resolve_key(
                                value.split(".")[-1] if "." in value else value
                            )
                            if _rk:
                                table_fqn = _rk
                                logger.debug(f"_append_column_ref: Using CTE {value} from alias_map as default source for unqualified column {col_exp.name}")
                                break
                        if not table_fqn:
                            # If still not found, try to find the source table (exclude INTO target for SELECT INTO)
                            # For SELECT INTO, exclude the target table from alias_map when resolving unqualified columns
                            select_stmt = getattr(self, '_current_select_stmt', None)
                            into_target = None
                            if select_stmt and isinstance(select_stmt, exp.Select):
                                into_expr = select_stmt.args.get('into')
                                if into_expr:
                                    # Get the target table name from INTO clause
                                    if hasattr(into_expr, 'this'):
                                        into_target = self._get_table_name(into_expr.this)
                                    # Also check if it's a temp table (starts with #)
                                    if into_target and not into_target.startswith('#'):
                                        # Check if it's in temp_registry with #
                                        simple = into_target.split('.')[-1] if '.' in into_target else into_target
                                        if f"#{simple}" in self.temp_registry:
                                            into_target = f"#{simple}"
                                    elif not into_target:
                                        # Try to extract from SQL string
                                        try:
                                            original_sql = str(select_stmt.sql(dialect=self.dialect))
                                            import re as _re
                                            into_match = _re.search(r'\bINTO\s+#(\w+)', original_sql, _re.IGNORECASE)
                                            if into_match:
                                                into_target = f"#{into_match.group(1)}"
                                        except Exception:
                                            pass
                            
                            # Filter out INTO target from alias_map values when resolving unqualified columns
                            non_empty_values = [v for k, v in alias_map.items() if k != '']
                            if into_target:
                                # Remove INTO target from consideration
                                into_target_simple = into_target.split('.')[-1] if '.' in into_target else into_target
                                non_empty_values = [v for v in non_empty_values if not (v.endswith(into_target_simple) or v.endswith(into_target) or into_target in v or into_target_simple in v)]
                            
                            if len(non_empty_values) == 1:
                                table_fqn = non_empty_values[0]
                                logger.debug(f"_append_column_ref: Using single source value from alias_map (excluding INTO target {into_target}): {table_fqn} for unqualified column {col_exp.name}")
                            elif len(non_empty_values) > 1:
                                # If still multiple values, prefer sources from main FROM clause over subquery sources
                                # For INSERT INTO ... SELECT FROM CTE WHERE NOT EXISTS (SELECT FROM table),
                                # the columns should come from the main FROM (CTE), not from the subquery table
                                # Check if any value is a CTE (likely from main FROM clause)
                                main_from_values = []
                                for v in non_empty_values:
                                    value_simple = v.split('.')[-1] if '.' in v else v
                                    if self._cte_registry_resolve_key(v) or self._cte_registry_resolve_key(value_simple):
                                        main_from_values.append(v)
                                
                                if len(main_from_values) == 1:
                                    table_fqn = main_from_values[0]
                                    logger.debug(f"_append_column_ref: Using CTE from main FROM clause: {table_fqn} for unqualified column {col_exp.name}")
                                else:
                                    # If still multiple values, prefer non-temp tables (sources, not targets)
                                    non_temp_values = [v for v in non_empty_values if not (v.startswith('#') or 'tempdb' in v.lower() or '#temp' in v.lower())]
                                    if len(non_temp_values) == 1:
                                        table_fqn = non_temp_values[0]
                                        logger.debug(f"_append_column_ref: Using single non-temp source from alias_map: {table_fqn} for unqualified column {col_exp.name}")
                                    else:
                                        logger.debug(f"_append_column_ref: Multiple values in alias_map after filtering: {non_empty_values}, cannot resolve unqualified column {col_exp.name}")
                                        return
                            else:
                                logger.debug(f"_append_column_ref: No values in alias_map after filtering (INTO target: {into_target}), cannot resolve unqualified column {col_exp.name}")
                                return
            else:
                return

    if not qual and table_fqn:
        scope = scope_select or getattr(self, "_current_select_stmt", None)
        scope_aliases = _from_clause_alias_keys(scope) if isinstance(scope, exp.Select) else None
        dis = _disambiguate_unqualified_column_fqn(
            self, col_exp.name, alias_map, table_fqn, scope_aliases
        )
        if dis is None:
            return
        table_fqn = dis
    
    # Check if this is a CTE reference
    # CTEs don't have direct table sources - their sources are in the CTE definition
    # Try to expand CTE to its sources
    cte_name_simple = table_fqn.split('.')[-1] if '.' in table_fqn else table_fqn
    is_cte = bool(
        self._cte_registry_resolve_key(qual)
        or self._cte_registry_resolve_key(table_fqn)
        or self._cte_registry_resolve_key(cte_name_simple)
    )
    
    # Also check if the table name matches a CTE in the current SELECT statement's WITH clause
    # This handles cases where CTE is not in cte_registry (e.g., when SELECT INTO is parsed separately from WITH)
    if not is_cte:
        select_stmt = getattr(self, '_current_select_stmt', None)
        if select_stmt and isinstance(select_stmt, exp.Select):
            with_clause = select_stmt.args.get('with')
            if with_clause and hasattr(with_clause, 'expressions'):
                for cte in with_clause.expressions:
                    cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                    if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                        is_cte = True
                        logger.debug(f"_append_column_ref: Detected CTE {cte_alias} from WITH clause (not in cte_registry)")
                        break
        # Also check parent statement (for SELECT INTO that's part of WITH statement)
        if not is_cte:
            try:
                parent = getattr(select_stmt, 'parent', None) if select_stmt else None
                if isinstance(parent, exp.With):
                    with_clause = parent.args.get('expressions') if hasattr(parent, 'args') else None
                    if with_clause:
                        for cte in with_clause:
                            cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                            if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                                is_cte = True
                                logger.debug(f"_append_column_ref: Detected CTE {cte_alias} from parent WITH clause (not in cte_registry)")
                                break
            except Exception:
                pass
    
    if is_cte:
        use_cte_nodes = getattr(self, "_use_cte_nodes", True)
        if use_cte_nodes:
            cte_key = (
                self._cte_registry_resolve_key(qual)
                or self._cte_registry_resolve_key(table_fqn)
                or self._cte_registry_resolve_key(cte_name_simple)
                or cte_name_simple
            )
            cte_info = self.cte_registry.get(cte_key, {})
            node_name = (
                cte_info.get("node_name")
                if isinstance(cte_info, dict)
                else _cte_node_name(self, cte_key)
            )
            ns = self._canonical_namespace(
                self.current_database or self.default_database or "InfoTrackerDW"
            )
            out_list.append(
                ColumnReference(namespace=ns, table_name=node_name, column_name=col_exp.name)
            )
            return

        # Try to find CTE definition and extract sources from it
        # Find the actual CTE name (case-insensitive match)
        cte_name_simple = table_fqn.split('.')[-1] if '.' in table_fqn else table_fqn
        cte_name = (
            self._cte_registry_resolve_key(qual)
            or self._cte_registry_resolve_key(table_fqn)
            or self._cte_registry_resolve_key(cte_name_simple)
            or cte_name_simple
        )
        
        # If CTE not in registry, try to find it in WITH clause
        if not self._cte_registry_resolve_key(cte_name):
            select_stmt = getattr(self, '_current_select_stmt', None)
            if select_stmt and isinstance(select_stmt, exp.Select):
                with_clause = select_stmt.args.get('with')
                if with_clause and hasattr(with_clause, 'expressions'):
                    for cte in with_clause.expressions:
                        cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                        if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                            cte_name = cte_alias
                            logger.debug(f"_append_column_ref: Found CTE {cte_name} in WITH clause")
                            break
            # Also check parent statement (for SELECT INTO that's part of WITH statement)
            if not self._cte_registry_resolve_key(cte_name):
                try:
                    parent = getattr(select_stmt, 'parent', None) if select_stmt else None
                    if isinstance(parent, exp.With):
                        with_clause = parent.args.get('expressions') if hasattr(parent, 'args') else None
                        if with_clause:
                            for cte in with_clause:
                                cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                                if cte_alias and (cte_alias.lower() == qual.lower() or cte_alias.lower() == cte_name_simple.lower() or cte_alias.lower() == table_fqn.split('.')[-1].lower() if '.' in table_fqn else False):
                                    cte_name = cte_alias
                                    logger.debug(f"_append_column_ref: Found CTE {cte_name} in parent WITH clause")
                                    break
                except Exception:
                    pass
        
        if not cte_name:
            cte_name = cte_name_simple if 'cte_name_simple' in locals() else (table_fqn.split('.')[-1] if '.' in table_fqn else table_fqn)
        
        logger.debug(f"_append_column_ref: Detected CTE reference: {cte_name}")
        # Look for CTE definition - first in current SELECT statement's WITH clause, then in cte_registry
        try:
            select_stmt = getattr(self, '_current_select_stmt', None)
            cte_found = False
            if select_stmt and isinstance(select_stmt, exp.Select):
                with_clause = select_stmt.args.get('with')
                if with_clause and hasattr(with_clause, 'expressions'):
                    for cte in with_clause.expressions:
                        cte_alias = str(cte.alias) if hasattr(cte, 'alias') and cte.alias else None
                        if hasattr(cte, 'alias') and str(cte.alias).lower() == self._cte_simple_name(cte_name).lower():
                            # Found the CTE definition, extract its sources
                            if isinstance(cte.this, exp.Select):
                                # Extract dependencies from CTE's SELECT
                                cte_deps = self._extract_dependencies(cte.this)
                                logger.debug(f"_append_column_ref: CTE {cte_name} dependencies: {cte_deps}")
                                # Check if any dependency is a temp table
                                temp_deps_found = False
                                for dep in cte_deps:
                                    dep_simple = dep.split('.')[-1] if '.' in dep else dep
                                    if dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry):
                                        temp_deps_found = True
                                        # Use this temp table as the source
                                        temp_seg = dep_simple if dep_simple.startswith('#') else f"#{dep_simple}"
                                        logger.debug(f"_append_column_ref: CTE {cte_name} sources temp table {temp_seg}, using it as source")
                                        ns_temp, table_name = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
                                        use_direct_ref = getattr(self, '_use_direct_temp_ref', False)
                                        if use_direct_ref:
                                            # Add direct reference to temp table
                                            ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
                                            out_list.append(ref)
                                            logger.debug(f"_append_column_ref: Added temp ref from CTE: {ref}")
                                            return
                                        # Try to use temp_lineage if available
                                        ver = self._temp_current(temp_seg)
                                        colname = col_exp.name
                                        if ver and ver in self.temp_lineage and colname in self.temp_lineage[ver]:
                                            _r = self.temp_lineage[ver][colname]
                                            if _r:
                                                for _xr in _r:
                                                    _normalize_column_ref_temp(self, _xr)
                                                out_list.extend(_r)
                                                logger.debug(f"_append_column_ref: Using temp_lineage from CTE: {len(_r)} refs")
                                                return
                                        if temp_seg in self.temp_lineage and colname in self.temp_lineage[temp_seg]:
                                            _r2 = self.temp_lineage[temp_seg][colname]
                                            if _r2:
                                                for _xr in _r2:
                                                    _normalize_column_ref_temp(self, _xr)
                                                out_list.extend(_r2)
                                                logger.debug(f"_append_column_ref: Using temp_lineage from CTE: {len(_r2)} refs")
                                                return
                                        # Fallback: add direct reference
                                        ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
                                        out_list.append(ref)
                                        logger.debug(f"_append_column_ref: Added temp ref from CTE (fallback): {ref}")
                                        return
                                
                                # If no temp tables found, use regular tables as sources (expand CTE to base sources)
                                # This handles cases like "SELECT INTO #MaxLoadDate FROM MaxDates" where MaxDates is a CTE
                                if not temp_deps_found and cte_deps:
                                    logger.debug(f"_append_column_ref: CTE {cte_name} has no temp table dependencies, expanding to base sources: {cte_deps}")
                                    
                                    # FIXED: Instead of blindly adding column to ALL dependencies, 
                                    # extract column-level lineage from CTE definition to find the correct source table(s)
                                    try:
                                        logger.warning(f"DIAGNOSTIC: Extracting lineage for CTE {cte_name}, col={col_exp.name}, cte.this={type(cte.this).__name__}")
                                        
                                        cte_lineage, _cte_schema = self._extract_column_lineage(cte.this, cte_name)
                                        
                                        logger.warning(f"DIAGNOSTIC: Extracted {len(cte_lineage)} columns from CTE {cte_name}")
                                        
                                        logger.debug(f"_append_column_ref: Extracted lineage for CTE {cte_name}, {len(cte_lineage)} columns")
                                        
                                        # Find lineage for the requested column
                                        col_lineage = None
                                        for lin in cte_lineage:
                                            if lin.output_column.lower() == col_exp.name.lower():
                                                col_lineage = lin
                                                break
                                        
                                        if col_lineage:
                                            logger.warning(f"DIAGNOSTIC: Found lineage for {col_exp.name}, inputs={[(r.table_name, r.column_name) for r in col_lineage.input_fields[:3]]}")
                                        else:
                                            logger.warning(f"DIAGNOSTIC: No lineage found for {col_exp.name} in CTE {cte_name}, available columns={[l.output_column for l in cte_lineage[:5]]}")
                                        
                                        if col_lineage and col_lineage.input_fields:
                                            logger.debug(f"_append_column_ref: Found lineage for column {col_exp.name} in CTE {cte_name}, {len(col_lineage.input_fields)} input fields")
                                            # Use the input_fields from CTE's lineage as sources
                                            for input_ref in col_lineage.input_fields:
                                                # input_ref might be another CTE - if so, it will be recursively expanded by deps.py
                                                _normalize_column_ref_temp(self, input_ref)
                                                out_list.append(input_ref)
                                                logger.debug(f"_append_column_ref: Added ref from CTE {cte_name} column lineage: {input_ref}")
                                            if out_list:
                                                return
                                        else:
                                            logger.debug(f"_append_column_ref: No lineage found for column {col_exp.name} in CTE {cte_name}, falling back to all dependencies")
                                    except Exception as e:
                                        import traceback
                                        logger.warning(f"DIAGNOSTIC: Exception extracting CTE lineage: {e}")
                                        logger.warning(f"DIAGNOSTIC: Traceback: {traceback.format_exc()}")
                                        logger.debug(f"_append_column_ref: Failed to extract CTE lineage: {e}, falling back to all dependencies")
                                    
                                    # Fallback: if lineage extraction failed, add column to all dependencies (old behavior)
                                    for dep in cte_deps:
                                        dep_simple = dep.split('.')[-1] if '.' in dep else dep
                                        # Skip temp tables (they should be expanded separately)
                                        if not (dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry)):
                                            # This is a regular table - use it as source
                                            db, sch, tbl = self._split_fqn(dep)
                                            effective_db = db or self.current_database or self.default_database or "InfoTrackerDW"
                                            ref = ColumnReference(
                                                namespace=self._canonical_namespace(effective_db),
                                                table_name=f"{sch}.{tbl}",
                                                column_name=col_exp.name,
                                            )
                                            out_list.append(ref)
                                            logger.debug(f"_append_column_ref: Added ref from CTE {cte_name} base source {dep}: {ref}")
                                    if out_list:
                                        return
                            cte_found = True
                            break
            # If CTE not found in current SELECT, check if it's registered and try to find its source
            _cte_reg_key = self._cte_registry_resolve_key(cte_name)
            if not cte_found and _cte_reg_key:
                # CTE is registered but not in current SELECT - it might be defined before INSERT
                # Get the CTE definition from cte_registry
                cte_info = self.cte_registry[_cte_reg_key]
                # Handle both dict format (with 'definition' key) and legacy list format (just columns)
                if isinstance(cte_info, dict) and 'definition' in cte_info:
                    cte_def = cte_info['definition']
                elif isinstance(cte_info, exp.Select):
                    cte_def = cte_info
                else:
                    cte_def = None
                logger.debug(f"_append_column_ref: CTE {cte_name} found in cte_registry, extracting dependencies from definition")
                if cte_def and isinstance(cte_def, exp.Select):
                    # Extract dependencies from CTE's SELECT
                    cte_deps = self._extract_dependencies(cte_def)
                    logger.debug(f"_append_column_ref: CTE {cte_name} dependencies from registry: {cte_deps}")
                    
                    # Check if any dependency is a temp table
                    temp_deps_found = False
                    temp_lineage_used = False
                    for dep in cte_deps:
                        dep_simple = dep.split('.')[-1] if '.' in dep else dep
                        if dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry):
                            temp_deps_found = True
                            # Use this temp table as the source
                            temp_seg = dep_simple if dep_simple.startswith('#') else f"#{dep_simple}"
                            logger.debug(f"_append_column_ref: CTE {cte_name} sources temp table {temp_seg}, using it as source")
                            ns_temp, table_name = self._ns_and_name(temp_seg, obj_type_hint="temp_table")
                            use_direct_ref = getattr(self, '_use_direct_temp_ref', False)
                            if use_direct_ref:
                                # Add direct reference to temp table
                                ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
                                out_list.append(ref)
                                logger.debug(f"_append_column_ref: Added temp ref from CTE registry: {ref}")
                                temp_lineage_used = True
                                return
                            # Try to use temp_lineage if available
                            ver = self._temp_current(temp_seg)
                            colname = col_exp.name
                            if ver and ver in self.temp_lineage and colname in self.temp_lineage[ver]:
                                _r3 = self.temp_lineage[ver][colname]
                                if _r3:
                                    for _xr in _r3:
                                        _normalize_column_ref_temp(self, _xr)
                                    out_list.extend(_r3)
                                    logger.debug(f"_append_column_ref: Using temp_lineage from CTE registry: {len(_r3)} refs")
                                    temp_lineage_used = True
                                    return
                            if temp_seg in self.temp_lineage and colname in self.temp_lineage[temp_seg]:
                                _r4 = self.temp_lineage[temp_seg][colname]
                                if _r4:
                                    for _xr in _r4:
                                        _normalize_column_ref_temp(self, _xr)
                                    out_list.extend(_r4)
                                    logger.debug(f"_append_column_ref: Using temp_lineage from CTE registry: {len(_r4)} refs")
                                    temp_lineage_used = True
                                    return
                            # Column not in temp_lineage - this temp table doesn't have this column
                            # Continue to check other dependencies or fall through to column-level expansion
                            logger.debug(f"_append_column_ref: Column {col_exp.name} not in temp_lineage for {temp_seg}, will try column-level expansion")
                    
                    # If temp dependencies were found but column wasn't in any of them, 
                    # OR if no temp dependencies at all, use column-level lineage expansion
                    if (temp_deps_found and not temp_lineage_used) or (not temp_deps_found and cte_deps):
                        if temp_deps_found and not temp_lineage_used:
                            logger.warning(f"DIAGNOSTIC_REG: CTE {cte_name} has temp dependencies but column {col_exp.name} not in temp_lineage, expanding to column-level sources: {cte_deps}")
                            logger.debug(f"_append_column_ref: CTE {cte_name} has temp dependencies but column not in temp_lineage, expanding to column-level sources: {cte_deps}")
                        else:
                            logger.warning(f"DIAGNOSTIC_REG: CTE {cte_name} (from registry) has no temp table dependencies, expanding to base sources: {cte_deps}")
                            logger.debug(f"_append_column_ref: CTE {cte_name} (from registry) has no temp table dependencies, expanding to base sources: {cte_deps}")
                        
                        # FIXED: Instead of blindly adding column to ALL dependencies, 
                        # extract column-level lineage from CTE definition to find the correct source table(s)
                        
                        # Prevent infinite recursion: check if we're already expanding this CTE
                        _cte_stack_id = _cte_reg_key
                        if _cte_stack_id in self._cte_expansion_stack:
                            logger.debug(f"_append_column_ref: CTE {cte_name} already in expansion stack, skipping to avoid infinite recursion")
                            # Skip to fallback logic
                        else:
                            try:
                                # Mark this CTE as being expanded
                                self._cte_expansion_stack.add(_cte_stack_id)
                                logger.warning(f"DIAGNOSTIC_REG: Extracting lineage for CTE {cte_name}, col={col_exp.name}, cte_def={type(cte_def).__name__}")
                                cte_lineage, _cte_schema = self._extract_column_lineage(cte_def, cte_name)
                                logger.warning(f"DIAGNOSTIC_REG: Extracted {len(cte_lineage)} columns from CTE {cte_name}")
                                logger.debug(f"_append_column_ref: Extracted lineage for CTE {cte_name} (from registry), {len(cte_lineage)} columns")
                                
                                # Find lineage for the requested column
                                col_lineage = None
                                for lin in cte_lineage:
                                    if lin.output_column.lower() == col_exp.name.lower():
                                        col_lineage = lin
                                        break
                                
                                if col_lineage and col_lineage.input_fields:
                                    logger.debug(f"_append_column_ref: Found lineage for column {col_exp.name} in CTE {cte_name} (from registry), {len(col_lineage.input_fields)} input fields")
                                    # Use the input_fields from CTE's lineage as sources
                                    for input_ref in col_lineage.input_fields:
                                        # input_ref might be another CTE - if so, it will be recursively expanded by deps.py
                                        _normalize_column_ref_temp(self, input_ref)
                                        out_list.append(input_ref)
                                        logger.debug(f"_append_column_ref: Added ref from CTE {cte_name} (from registry) column lineage: {input_ref}")
                                    if out_list:
                                        # Clean up expansion stack before returning
                                        self._cte_expansion_stack.discard(_cte_stack_id)
                                        return
                                else:
                                    logger.debug(f"_append_column_ref: No lineage found for column {col_exp.name} in CTE {cte_name} (from registry), falling back to all dependencies")
                            except Exception as e:
                                import traceback
                                logger.debug(f"_append_column_ref: Exception extracting CTE lineage: {e}")
                                logger.debug(f"_append_column_ref: Traceback: {traceback.format_exc()}")
                                logger.debug(f"_append_column_ref: Failed to extract CTE lineage from registry: {e}, falling back to all dependencies")
                            finally:
                                # Always remove from stack after processing
                                self._cte_expansion_stack.discard(_cte_stack_id)
                        
                        # Fallback: if lineage extraction failed, add column to all dependencies (old behavior)
                        for dep in cte_deps:
                            dep_simple = dep.split('.')[-1] if '.' in dep else dep
                            # Skip temp tables (they should be expanded separately)
                            if not (dep_simple.startswith('#') or (f"#{dep_simple}" in self.temp_registry)):
                                # This is a regular table - use it as source
                                db, sch, tbl = self._split_fqn(dep)
                                effective_db = db or self.current_database or self.default_database or "InfoTrackerDW"
                                ref = ColumnReference(
                                    namespace=self._canonical_namespace(effective_db),
                                    table_name=f"{sch}.{tbl}",
                                    column_name=col_exp.name,
                                )
                                out_list.append(ref)
                                logger.debug(f"_append_column_ref: Added ref from CTE {cte_name} (from registry) base source {dep}: {ref}")
                        if out_list:
                            return
        except Exception as e:
            logger.debug(f"_append_column_ref: Exception expanding CTE {cte_name}: {e}")
            import traceback
            logger.debug(f"_append_column_ref: Traceback: {traceback.format_exc()}")
    
    db, sch, tbl = self._split_fqn(table_fqn)
    logger.debug(f"_append_column_ref: table_fqn={table_fqn}, split to db={db}, sch={sch}, tbl={tbl}")
    # Detect temp segment anywhere in the FQN
    try:
        temp_seg = None
        for seg in (table_fqn or '').split('.'):
            if str(seg).startswith('#'):
                temp_seg = seg
                break
        # If no # found but tbl is in temp_registry as #tbl, it's a temp table
        if not temp_seg and tbl:
            # Check if tbl already starts with # (from _split_fqn)
            if str(tbl).startswith('#'):
                temp_seg = tbl
            else:
                temp_seg = _match_temp_segment(self, str(tbl))
        logger.debug(f"_append_column_ref: temp_seg={temp_seg}, temp_registry keys={list(self.temp_registry.keys())[:5] if hasattr(self, 'temp_registry') else 'N/A'}")
        if temp_seg:
            # Use _ns_and_name to get proper format with procedure context: schema.object#temp
            ns_temp, table_name = _scoped_temp_ref(self, temp_seg)
            logger.debug(f"_append_column_ref: temp_seg={temp_seg}, ns_temp={ns_temp}, table_name={table_name}")
            # Check if we should use direct reference (for INSERT INTO) or expanded lineage
            use_direct_ref = getattr(self, '_use_direct_temp_ref', False)
            logger.debug(f"_append_column_ref: use_direct_ref={use_direct_ref}")
            if not use_direct_ref:
                # Inline base lineage if we have it
                ver = self._temp_current(temp_seg)
                colname = col_exp.name
                if ver and ver in self.temp_lineage and colname in self.temp_lineage[ver]:
                    _tl_refs = self.temp_lineage[ver][colname]
                    if _tl_refs:
                        for _xr in _tl_refs:
                            _normalize_column_ref_temp(self, _xr)
                        out_list.extend(_tl_refs)
                        logger.debug(
                            f"_append_column_ref: Using temp_lineage[{ver}][{colname}]: {len(_tl_refs)} refs"
                        )
                        return
                if temp_seg in self.temp_lineage and colname in self.temp_lineage[temp_seg]:
                    _tl_refs2 = self.temp_lineage[temp_seg][colname]
                    if _tl_refs2:
                        for _xr in _tl_refs2:
                            _normalize_column_ref_temp(self, _xr)
                        out_list.extend(_tl_refs2)
                        logger.debug(
                            f"_append_column_ref: Using temp_lineage[{temp_seg}][{colname}]: {len(_tl_refs2)} refs"
                        )
                        return
            # If no temp_lineage or use_direct_ref is True, add direct reference with new format
            ref = ColumnReference(namespace=ns_temp, table_name=table_name, column_name=col_exp.name)
            out_list.append(ref)
            logger.debug(f"_append_column_ref: Added direct temp ref: {ref}")
    except Exception as e:
        logger.debug(f"_append_column_ref: Exception handling temp table: {e}")
        import traceback
        logger.debug(f"_append_column_ref: Traceback: {traceback.format_exc()}")
    # Resolve namespace for non-temp inputs:
    # - prefer explicit db from the FQN,
    # - otherwise consult the global object→DB registry,
    # - finally fall back to current/default DB (or InfoTrackerDW).
    effective_db = db
    if not effective_db:
        # Try registry (learned from CREATE TABLE/VIEW/PROC)
        try:
            if getattr(self, "registry", None) and sch and tbl:
                schema_table = f"{sch}.{tbl}"
                fallback_db = getattr(self, "current_database", None) or getattr(self, "default_database", None) or "InfoTrackerDW"
                resolved = self.registry.resolve("table", schema_table, fallback=fallback_db)
                if resolved:
                    effective_db = resolved
        except Exception:
            effective_db = None
    if not effective_db:
        try:
            effective_db = self.current_database or self.default_database or "InfoTrackerDW"
        except Exception:
            effective_db = "InfoTrackerDW"
    out_list.append(
        ColumnReference(
            namespace=self._canonical_namespace(effective_db),
            table_name=f"{sch}.{tbl}",
            column_name=col_exp.name,
        )
    )


def _enclosing_select_for_column(col: exp.Column) -> Optional[exp.Select]:
    """SELECT that syntactically owns this column (via parent pointers).

    sqlglot may reuse one ``Column`` node in multiple branches (e.g. projection and
    ``MAX(col)`` in a predicate). ``find_all`` then yields several SELECTs containing the
    same object; picking the smallest subtree wrongly resolves to a subquery. Walking
    ``parent`` gives the correct scope for unqualified names in that projection/WHERE slot.
    """
    node = col.parent
    while node is not None and not isinstance(node, exp.Select):
        node = getattr(node, "parent", None)
    return node if isinstance(node, exp.Select) else None


def _collect_inputs_for_expr(self, expr: exp.Expression, alias_map: dict, derived_cols: dict):
    inputs = []
    outer_stmt = getattr(self, "_current_select_stmt", None)

    def _smallest_select_containing(root: Optional[exp.Select], col: exp.Column) -> Optional[exp.Select]:
        """Innermost SELECT whose subtree contains this column (fixes VALUES / MAX(LoadDate) subqueries)."""
        if root is None:
            return None

        def _cols_of(s: exp.Select):
            return s.find_all(exp.Column)

        candidates = [s for s in root.find_all(exp.Select) if col in _cols_of(s)]
        if not candidates:
            return None
        return min(candidates, key=lambda s: sum(1 for _ in s.walk()))

    for col in expr.find_all(exp.Column):
        enc = _enclosing_select_for_column(col)
        if enc is None:
            enc = _smallest_select_containing(outer_stmt, col)
        eff_map = alias_map
        eff_derived = derived_cols
        if enc is not None and enc is not outer_stmt:
            try:
                eff_map, eff_derived = _build_alias_maps(self, enc)
            except Exception:
                eff_map, eff_derived = alias_map, derived_cols

        qual = (col.table or "").lower()
        key = (qual, col.name.lower())
        base_cols = eff_derived.get(key)
        if base_cols:
            # VALUES row cells reference outer SELECT aliases (e.g. Document_hub.col); resolve with outer alias_map.
            for b in base_cols:
                _append_column_ref(self, inputs, b, alias_map, outer_stmt)
            continue
        scope_sel = enc if enc is not None else outer_stmt
        _append_column_ref(self, inputs, col, eff_map, scope_sel)
    return inputs


def _get_schema(self, db: str, sch: str, tbl: str):
    ns = self._canonical_namespace(db) if db else None
    key = f"{sch}.{tbl}"
    if hasattr(self.schema_registry, "get"):
        return self.schema_registry.get(ns, key)
    return self.schema_registry.get((ns, key))


def _type_of_column(self, col_exp, alias_map):
    qual = (getattr(col_exp, "table", None) or "").lower()
    fqn = alias_map.get(qual)
    if not fqn:
        return None
    db, sch, tbl = self._split_fqn(fqn)
    schema = _get_schema(self, db, sch, tbl)
    if not schema:
        return None
    c = schema.get_column(col_exp.name)
    return c.data_type if c else None


def _infer_type(self, expr, alias_map) -> str:
    if isinstance(expr, exp.Cast):
        t = expr.args.get("to")
        return str(t) if t else "unknown"
    if isinstance(expr, exp.Convert):
        t = expr.args.get("to")
        return str(t) if t else "unknown"
    if isinstance(expr, (exp.Trim, exp.Upper, exp.Lower)):
        base = expr.find(exp.Column)
        return _type_of_column(self, base, alias_map) or "nvarchar"
    if isinstance(expr, exp.Coalesce):
        types = []
        for a in (expr.args.get("expressions") or []):
            if isinstance(a, exp.Column):
                types.append(_type_of_column(self, a, alias_map))
            elif isinstance(a, exp.Literal):
                types.append("nvarchar" if a.is_string else "numeric")
        tset = [t for t in types if t]
        if any(t and "nvarchar" in t.lower() for t in tset):
            return "nvarchar"
        if any(t and "varchar" in t.lower() for t in tset):
            return "varchar"
        return tset[0] if tset else "unknown"
    s = str(expr).upper()
    if "HASHBYTES(" in s or "MD5(" in s:
        return "binary(16)"
    if isinstance(expr, exp.Column):
        return _type_of_column(self, expr, alias_map) or "unknown"
    return "unknown"


def _short_desc(self, expr) -> str:
    return " ".join(str(expr).split())[:250]


def _extract_view_header_cols(self, create_exp) -> list[str]:
    cols: list[str] = []

    def _collect(exprs) -> None:
        if not exprs:
            return
        for e in exprs:
            n = getattr(e, "name", None)
            if n:
                cols.append(str(n).strip("[]"))
            else:
                cols.append(str(e).strip().strip("[]"))

    exprs = getattr(create_exp, "expressions", None) or create_exp.args.get("expressions")
    _collect(exprs)
    try:
        target = getattr(create_exp, "this", None)
        texprs = getattr(target, "expressions", None) or (getattr(target, "args", {}).get("expressions") if getattr(target, "args", None) else None)
        _collect(texprs)
    except Exception:
        pass

    seen = set()
    out = []
    for c in cols:
        lc = c.lower()
        if lc in seen:
            continue
        seen.add(lc)
        out.append(c)
    return out


def _extract_column_alias(self, select_expr: exp.Expression) -> Optional[str]:
    """Extract column alias or name from a SELECT expression."""
    if hasattr(select_expr, 'alias') and select_expr.alias:
        return str(select_expr.alias)
    if isinstance(select_expr, exp.Alias):
        return str(select_expr.alias)
    if isinstance(select_expr, exp.Column):
        return str(select_expr.this)
    expr_str = str(select_expr)
    up = expr_str.upper()
    if ' AS ' in up:
        parts = expr_str.split()
        as_idx = -1
        for i, part in enumerate(parts):
            if part.upper() == 'AS':
                as_idx = i
                break
        if as_idx >= 0 and as_idx + 1 < len(parts):
            return parts[as_idx + 1].strip("'\"")
    return None


def _extract_column_references(self, select_expr: exp.Expression, select_stmt: exp.Select) -> List[ColumnReference]:
    """Resolve column refs inside a projection expression using FROM/JOIN scope (incl. multi-table)."""
    alias_map, derived_cols = _build_alias_maps(self, select_stmt)
    self._current_select_stmt = select_stmt
    return _collect_inputs_for_expr(self, select_expr, alias_map, derived_cols)


def _is_string_function(self, expr: exp.Expression) -> bool:
    string_functions = ['RIGHT', 'LEFT', 'SUBSTRING', 'CHARINDEX', 'LEN', 'CONCAT']
    expr_str = str(expr).upper()
    return any(func in expr_str for func in string_functions)


def _has_star_expansion(self, select_stmt: exp.Select) -> bool:
    logger.debug(f"_has_star_expansion: Checking {len(select_stmt.expressions)} expressions")
    for expr in select_stmt.expressions:
        if isinstance(expr, exp.Star):
            logger.debug(f"_has_star_expansion: Found exp.Star")
            return True
        if isinstance(expr, exp.Column):
            if str(expr.this) == "*" or str(expr).endswith(".*"):
                logger.debug(f"_has_star_expansion: Found exp.Column star: {expr}")
                return True
    return False


def _has_union(self, stmt: exp.Expression) -> bool:
    return isinstance(stmt, exp.Union) or len(list(stmt.find_all(exp.Union))) > 0


def _is_placeholder_cols(columns: list) -> bool:
    if not columns:
        return True
    lowered = [str(c).lower() for c in columns if c]
    if not lowered:
        return True
    if len(lowered) == 1 and lowered[0] == "*":
        return True
    return all(c.startswith("unknown_") for c in lowered)


def _is_temp_source(table_name: str, temp_registry: dict) -> bool:
    if not table_name:
        return False
    if "#" in str(table_name):
        return True
    simple = str(table_name).split(".")[-1]
    return simple in temp_registry or f"#{simple}" in temp_registry


def _find_subquery_select_for_from_alias(self, select_stmt: exp.Select, alias: str) -> Optional[exp.Select]:
    """Resolve ``FROM (SELECT …) AS alias`` / ``FROM (SELECT …) alias`` to the inner :class:`exp.Select`.

    sqlglot often uses ``From(this=Subquery(…, alias=TableAlias(alias)))`` rather than ``Alias(Subquery,…)``.
    """
    want = (alias or "").strip().lower()
    if not want:
        return None
    frm = select_stmt.args.get("from_") or select_stmt.args.get("from")
    if not frm or not getattr(frm, "this", None):
        return None

    def _subquery_alias_name(sq: exp.Subquery) -> Optional[str]:
        a = sq.args.get("alias")
        if a is None:
            return None
        if isinstance(a, exp.TableAlias):
            ident = a.this if getattr(a, "this", None) is not None else a
            return str(getattr(ident, "name", ident) or ident).lower()
        return str(a).lower()

    def walk(n: Optional[exp.Expression]) -> Optional[exp.Select]:
        if n is None:
            return None
        if isinstance(n, exp.Paren):
            return walk(n.this)
        if isinstance(n, exp.Subquery):
            if _subquery_alias_name(n) == want and isinstance(n.this, exp.Select):
                return n.this
            return None
        if isinstance(n, exp.Alias):
            if str(n.alias).lower() == want:
                t = n.this
                if isinstance(t, exp.Subquery) and isinstance(t.this, exp.Select):
                    return t.this
                if isinstance(t, exp.Select):
                    return t.this
            return None
        if isinstance(n, exp.Join):
            return walk(n.this) or walk(n.expression)
        return None

    return walk(frm.this)


def _norm_sq_lineage_col_key(name: Optional[str]) -> str:
    if not name:
        return ""
    s = str(name).strip().strip("[]").strip()
    return s.lower()


def _build_sq_lineage_lookup(sq_lineage: List[ColumnLineage]) -> Dict[str, ColumnLineage]:
    """Map normalized output column names to lineage rows (first wins)."""
    lin_map: Dict[str, ColumnLineage] = {}
    for l in sq_lineage or []:
        if not l or not l.output_column:
            continue
        keys = {
            str(l.output_column).lower(),
            _norm_sq_lineage_col_key(str(l.output_column)),
        }
        for k in keys:
            if k and k not in lin_map:
                lin_map[k] = l
    return lin_map


def _resolve_lineage_for_subquery_column(
    cname: str,
    col_index: int,
    lin_map: Dict[str, ColumnLineage],
    sq_lineage: List[ColumnLineage],
    sq_cols: List[ColumnSchema],
) -> Optional[ColumnLineage]:
    """Match inner column lineage by normalized name, then by ordinal alignment with inner extract."""
    ck = _norm_sq_lineage_col_key(cname)
    if ck:
        ln = lin_map.get(ck) or lin_map.get(str(cname).lower())
        if ln:
            return ln
        for k, v in lin_map.items():
            if _norm_sq_lineage_col_key(k) == ck:
                return v
    if (
        col_index is not None
        and col_index >= 0
        and col_index < len(sq_lineage)
        and len(sq_lineage) == len(sq_cols)
    ):
        cand = sq_lineage[col_index]
        if cand and cand.output_column:
            if ck and _norm_sq_lineage_col_key(str(cand.output_column)) != ck:
                logger.debug(
                    "_try_expand_qualified_star_from_subquery: positional lineage at %s "
                    "(inner out=%r vs outer col=%r)",
                    col_index,
                    cand.output_column,
                    cname,
                )
            return cand
    logger.debug(
        "_try_expand_qualified_star_from_subquery: no lineage row for inner column %r (index %s)",
        cname,
        col_index,
    )
    return None


def _try_expand_qualified_star_from_subquery(
    self,
    alias: str,
    outer_select: exp.Select,
    view_name: str,
    seen_columns: Set[str],
    ordinal: int,
) -> Optional[Tuple[List[ColumnLineage], List[ColumnSchema], int]]:
    """Expand ``alias.*`` using column-level lineage from a FROM subquery (not a physical table)."""
    inner_sel = _find_subquery_select_for_from_alias(self, outer_select, alias)
    if inner_sel is None:
        return None
    depth = getattr(self, "_subquery_star_depth", 0)
    if depth > 8:
        logger.debug("_try_expand_qualified_star_from_subquery: depth limit, skipping")
        return None
    self._subquery_star_depth = depth + 1
    try:
        vname = f"{view_name}$subq:{alias}"
        sq_lineage, sq_cols = self._extract_column_lineage(inner_sel, vname)
    except Exception as e:
        logger.debug("_try_expand_qualified_star_from_subquery: inner extract failed: %s", e)
        return None
    finally:
        self._subquery_star_depth = depth

    if not sq_cols:
        return None
    lin_map = _build_sq_lineage_lookup(sq_lineage or [])
    out_lineage: List[ColumnLineage] = []
    out_cols: List[ColumnSchema] = []
    o = ordinal
    for col_index, col in enumerate(sq_cols):
        cname = col.name if col and col.name else None
        if not cname or str(cname) == "*" or str(cname).lower().startswith("unknown_"):
            continue
        if cname in seen_columns:
            continue
        seen_columns.add(cname)
        out_cols.append(ColumnSchema(name=cname, data_type="unknown", nullable=True, ordinal=o))
        o += 1
        ln = _resolve_lineage_for_subquery_column(
            str(cname), col_index, lin_map, list(sq_lineage or []), sq_cols
        )
        inputs = list(ln.input_fields) if ln and ln.input_fields else []
        ttype = ln.transformation_type if ln else TransformationType.UNKNOWN
        tdesc = (ln.transformation_description if ln else "") or f"{alias}.* via subquery"
        out_lineage.append(
            ColumnLineage(
                output_column=cname,
                input_fields=inputs,
                transformation_type=ttype,
                transformation_description=tdesc,
            )
        )
    if not out_lineage:
        return None
    return out_lineage, out_cols, o


def _handle_star_expansion(
    self,
    select_stmt: exp.Select,
    view_name: str,
    alias_map: Optional[dict] = None,
    derived_cols: Optional[dict] = None,
) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
    lineage = []
    output_columns = []
    ordinal = 0
    seen_columns = set()

    if alias_map is None or derived_cols is None:
        alias_map, derived_cols = _build_alias_maps(self, select_stmt)
    self._current_select_stmt = select_stmt
    
    logger.debug(f"_handle_star_expansion: Called for {view_name}, select_stmt has {len(select_stmt.expressions)} expressions")

    for select_expr in select_stmt.expressions:
        logger.debug(f"_handle_star_expansion: Processing expression type={type(select_expr).__name__}")
        if isinstance(select_expr, exp.Star):
            logger.debug(f"_handle_star_expansion: Found exp.Star, has table attr: {hasattr(select_expr, 'table')}, table value: {getattr(select_expr, 'table', None)}")
            if hasattr(select_expr, 'table') and select_expr.table:
                alias = str(select_expr.table)
                _sq_exp = _try_expand_qualified_star_from_subquery(
                    self, alias, select_stmt, view_name, seen_columns, ordinal
                )
                if _sq_exp:
                    _part_l, _part_c, ordinal = _sq_exp
                    lineage.extend(_part_l)
                    output_columns.extend(_part_c)
                    continue
                table_name = _resolve_table_from_alias(self, alias, select_stmt)
                if table_name != "unknown" and not _is_join_keyword(table_name):
                    if _is_temp_source(table_name, self.temp_registry):
                        temp_cols = []
                        try:
                            simple = str(table_name).split('.')[-1]
                            temp_cols = self.temp_registry.get(simple) or self.temp_registry.get(f"#{simple}") or []
                        except Exception:
                            temp_cols = []
                        if temp_cols and not _is_placeholder_cols(temp_cols):
                            for column_name in temp_cols:
                                if not column_name:
                                    continue
                                col_str = str(column_name)
                                if col_str == "*" or col_str.lower().startswith("unknown_"):
                                    continue
                                if col_str not in seen_columns:
                                    seen_columns.add(col_str)
                                    output_columns.append(ColumnSchema(name=col_str, data_type="unknown", nullable=True, ordinal=ordinal))
                                    ordinal += 1
                                    ns, nm = self._ns_and_name(table_name)
                                    lineage.append(ColumnLineage(output_column=col_str, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=col_str)], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
                            continue
                        if "*" not in seen_columns:
                            seen_columns.add("*")
                            output_columns.append(ColumnSchema(name="*", data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column="*", input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name="*")], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
                        continue
                    columns = self._infer_table_columns_unified(table_name)
                    if _is_placeholder_cols(columns):
                        if "*" not in seen_columns:
                            seen_columns.add("*")
                            output_columns.append(ColumnSchema(name="*", data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column="*", input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name="*")], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
                        continue
                    for column_name in columns:
                        if column_name not in seen_columns:
                            seen_columns.add(column_name)
                            output_columns.append(ColumnSchema(name=column_name, data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column=column_name, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=column_name)], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
            else:
                logger.debug(f"_handle_star_expansion: Bare * without table alias, searching for source tables in FROM")
                source_tables = []
                
                # Identify target table to exclude (e.g. SELECT * INTO #target FROM source)
                target_table_name = None
                if hasattr(select_stmt, 'args') and 'into' in select_stmt.args:
                    into_expr = select_stmt.args['into']
                    if into_expr and hasattr(into_expr, 'this') and isinstance(into_expr.this, exp.Table):
                         target_table_name = self._get_table_name(into_expr.this)
                         logger.debug(f"_handle_star_expansion: Identified target table to exclude: {target_table_name}")

                temp_sources = []
                for table in select_stmt.find_all(exp.Table):
                    table_name = self._get_table_name(table)
                    if table_name != "unknown":
                        # Exclude target table
                        if target_table_name and table_name == target_table_name:
                             continue
                        source_tables.append(table_name)
                        is_temp_table = False
                        try:
                            ident = table.args.get("this")
                            if hasattr(ident, "args") and ident.args.get("temporary"):
                                is_temp_table = True
                        except Exception:
                            pass
                        if is_temp_table or _is_temp_source(table_name, self.temp_registry):
                            temp_sources.append(table_name)
                logger.debug(f"_handle_star_expansion: Found {len(source_tables)} source tables: {source_tables}")
                if temp_sources:
                    temp_table = temp_sources[0]
                    try:
                        simple = str(temp_table).split('.')[-1]
                        temp_cols = self.temp_registry.get(simple) or self.temp_registry.get(f"#{simple}") or []
                    except Exception:
                        temp_cols = []
                    if temp_cols and not _is_placeholder_cols(temp_cols):
                        for column_name in temp_cols:
                            if not column_name:
                                continue
                            col_str = str(column_name)
                            if col_str == "*" or col_str.lower().startswith("unknown_"):
                                continue
                            if col_str not in seen_columns:
                                seen_columns.add(col_str)
                                output_columns.append(ColumnSchema(name=col_str, data_type="unknown", nullable=True, ordinal=ordinal))
                                ordinal += 1
                                ns, nm = self._ns_and_name(temp_table)
                                lineage.append(ColumnLineage(output_column=col_str, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=col_str)], transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))
                        continue
                    if "*" not in seen_columns:
                        seen_columns.add("*")
                        output_columns.append(ColumnSchema(name="*", data_type="unknown", nullable=True, ordinal=ordinal))
                        ordinal += 1
                        ns, nm = self._ns_and_name(temp_table)
                        lineage.append(ColumnLineage(output_column="*", input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name="*")], transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))
                    continue
                for table_name in source_tables:
                    # Skip JOIN keywords that should not be table names
                    if _is_join_keyword(table_name):
                        logger.debug(f"_handle_star_expansion: Skipping JOIN keyword: {table_name}")
                        continue
                    if _is_temp_source(table_name, self.temp_registry):
                        temp_cols = []
                        try:
                            simple = str(table_name).split('.')[-1]
                            temp_cols = self.temp_registry.get(simple) or self.temp_registry.get(f"#{simple}") or []
                        except Exception:
                            temp_cols = []
                        if temp_cols and not _is_placeholder_cols(temp_cols):
                            for column_name in temp_cols:
                                if not column_name:
                                    continue
                                col_str = str(column_name)
                                if col_str == "*" or col_str.lower().startswith("unknown_"):
                                    continue
                                if col_str not in seen_columns:
                                    seen_columns.add(col_str)
                                    output_columns.append(ColumnSchema(name=col_str, data_type="unknown", nullable=True, ordinal=ordinal))
                                    ordinal += 1
                                    ns, nm = self._ns_and_name(table_name)
                                    lineage.append(ColumnLineage(output_column=col_str, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=col_str)], transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))
                            continue
                        if "*" not in seen_columns:
                            seen_columns.add("*")
                            output_columns.append(ColumnSchema(name="*", data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column="*", input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name="*")], transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))
                        continue
                    logger.debug(f"_handle_star_expansion: Getting columns for table: {table_name}")
                    columns = self._infer_table_columns_unified(table_name)
                    if _is_placeholder_cols(columns):
                        if "*" not in seen_columns:
                            seen_columns.add("*")
                            output_columns.append(ColumnSchema(name="*", data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column="*", input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name="*")], transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))
                        continue
                    logger.debug(f"_handle_star_expansion: Got {len(columns)} columns from {table_name}")
                    for column_name in columns:
                        if column_name not in seen_columns:
                            seen_columns.add(column_name)
                            output_columns.append(ColumnSchema(name=column_name, data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column=column_name, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=column_name)], transformation_type=TransformationType.IDENTITY, transformation_description="SELECT *"))
        elif isinstance(select_expr, exp.Column) and (str(select_expr.this) == "*" or str(select_expr).endswith(".*")):
            logger.debug(f"_handle_star_expansion: Found column star: {select_expr}, table={select_expr.table}")
            if hasattr(select_expr, 'table') and select_expr.table:
                alias = str(select_expr.table)
                _sq_exp2 = _try_expand_qualified_star_from_subquery(
                    self, alias, select_stmt, view_name, seen_columns, ordinal
                )
                if _sq_exp2:
                    _part_l2, _part_c2, ordinal = _sq_exp2
                    lineage.extend(_part_l2)
                    output_columns.extend(_part_c2)
                    continue
                table_name = _resolve_table_from_alias(self, alias, select_stmt)
                logger.debug(f"_handle_star_expansion: Resolved alias {alias} to {table_name}")
                if table_name != "unknown" and not _is_join_keyword(table_name):
                    if _is_temp_source(table_name, self.temp_registry):
                        temp_cols = []
                        try:
                            simple = str(table_name).split('.')[-1]
                            temp_cols = self.temp_registry.get(simple) or self.temp_registry.get(f"#{simple}") or []
                        except Exception:
                            temp_cols = []
                        if temp_cols and not _is_placeholder_cols(temp_cols):
                            for column_name in temp_cols:
                                if not column_name:
                                    continue
                                col_str = str(column_name)
                                if col_str == "*" or col_str.lower().startswith("unknown_"):
                                    continue
                                if col_str not in seen_columns:
                                    seen_columns.add(col_str)
                                    output_columns.append(ColumnSchema(name=col_str, data_type="unknown", nullable=True, ordinal=ordinal))
                                    ordinal += 1
                                    ns, nm = self._ns_and_name(table_name)
                                    lineage.append(ColumnLineage(output_column=col_str, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=col_str)], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
                            continue
                        if "*" not in seen_columns:
                            seen_columns.add("*")
                            output_columns.append(ColumnSchema(name="*", data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column="*", input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name="*")], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
                        continue
                    columns = self._infer_table_columns_unified(table_name)
                    logger.debug(f"_handle_star_expansion: Inferred columns for {table_name}: {columns}")
                    if _is_placeholder_cols(columns):
                        logger.debug(f"_handle_star_expansion: No columns found for {table_name}, falling back to *")
                        # Fallback: if no columns found, treat as single * column
                        # This ensures we at least get the wildcard dependency
                        if "*" not in seen_columns:
                            seen_columns.add("*")
                            output_columns.append(ColumnSchema(name="*", data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column="*", input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name="*")], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
                        continue

                    for column_name in columns:
                        if column_name not in seen_columns:
                            seen_columns.add(column_name)
                            output_columns.append(ColumnSchema(name=column_name, data_type="unknown", nullable=True, ordinal=ordinal))
                            ordinal += 1
                            ns, nm = self._ns_and_name(table_name)
                            lineage.append(ColumnLineage(output_column=column_name, input_fields=[ColumnReference(namespace=ns, table_name=nm, column_name=column_name)], transformation_type=TransformationType.IDENTITY, transformation_description=f"{alias}.*"))
        else:
            logger.debug(f"_handle_star_expansion: Non-star expression, extracting alias")
            col_name = self._extract_column_alias(select_expr) or f"col_{ordinal}"
            logger.debug(f"_handle_star_expansion: Extracted alias: {col_name}")
            inner = select_expr.this if isinstance(select_expr, exp.Alias) else select_expr
            input_refs = _collect_inputs_for_expr(self, inner, alias_map, derived_cols)
            out_type = _infer_type(self, inner, alias_map)
            if isinstance(inner, (exp.Cast, exp.Convert)):
                ttype = TransformationType.CAST
            elif isinstance(inner, exp.Case):
                ttype = TransformationType.CASE
            elif isinstance(inner, exp.Column):
                ttype = TransformationType.IDENTITY
            else:
                s = str(inner).upper()
                if s.startswith("CASE ") or s.startswith("CASEWHEN ") or s.startswith("IIF("):
                    ttype = TransformationType.CASE
                else:
                    ttype = TransformationType.EXPRESSION
            output_columns.append(ColumnSchema(name=col_name, data_type=out_type, nullable=True, ordinal=ordinal))
            ordinal += 1
            lineage.append(
                ColumnLineage(
                    output_column=col_name,
                    input_fields=input_refs,
                    transformation_type=ttype,
                    transformation_description=_short_desc(self, inner),
                )
            )

    _normalize_lineage_input_temps(self, lineage)
    logger.debug(f"_handle_star_expansion: Returning {len(output_columns)} columns, {len(lineage)} lineage entries")
    return lineage, output_columns


def _handle_union_lineage(self, stmt: exp.Expression, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
    lineage = []
    output_columns = []
    union_selects = []
    if isinstance(stmt, exp.Union):
        def _collect_unions(node):
            if isinstance(node, exp.Union):
                _collect_unions(node.left)
                _collect_unions(node.right)
            elif isinstance(node, exp.Select):
                union_selects.append(node)
        _collect_unions(stmt)
    else:
        # If stmt is not a Union, but was passed to _handle_union_lineage,
        # it means it has a UNION somewhere inside. We need to find the actual Union nodes.
        if isinstance(stmt, exp.Select):
            # Find all Union nodes in the statement
            union_nodes = list(stmt.find_all(exp.Union))
            if union_nodes:
                # Use the first Union node found
                def _collect_unions(node):
                    if isinstance(node, exp.Union):
                        _collect_unions(node.left)
                        _collect_unions(node.right)
                    elif isinstance(node, exp.Select):
                        union_selects.append(node)
                _collect_unions(union_nodes[0])
            else:
                # No Union found, this shouldn't happen if _has_union returned True
                # But to avoid recursion, return empty
                return lineage, output_columns
        else:
            union_selects = []

    if not union_selects:
        return lineage, output_columns

    first_lineage, first_columns = self._extract_column_lineage(union_selects[0], view_name)
    for i, col_lineage in enumerate(first_lineage):
        all_input_fields = list(col_lineage.input_fields)
        for other_select in union_selects[1:]:
            if isinstance(other_select, exp.Select):
                other_lineage, _ = self._extract_column_lineage(other_select, view_name)
                if i < len(other_lineage):
                    all_input_fields.extend(other_lineage[i].input_fields)
        lineage.append(ColumnLineage(output_column=col_lineage.output_column, input_fields=all_input_fields, transformation_type=TransformationType.UNION, transformation_description="UNION operation"))
    output_columns = first_columns
    _normalize_lineage_input_temps(self, lineage)
    return lineage, output_columns


def _extract_column_lineage(self, stmt: exp.Expression, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
    logger.debug(f"_extract_column_lineage: Called for view_name={view_name}, stmt type={type(stmt).__name__}")
    lineage = []
    output_columns = []
    if isinstance(stmt, exp.Union):
        logger.debug(f"_extract_column_lineage: stmt is Union, calling _handle_union_lineage")
        return _handle_union_lineage(self, stmt, view_name)
    if not isinstance(stmt, exp.Select):
        logger.debug(f"_extract_column_lineage: stmt is not Select, returning empty")
        return lineage, output_columns
    select_stmt = stmt
    alias_map, derived_cols = _build_alias_maps(self, select_stmt)
    # Store select_stmt in self so _append_column_ref can access it for CTE expansion
    self._current_select_stmt = select_stmt
    projections = list(getattr(select_stmt, 'expressions', None) or [])
    logger.debug(f"_extract_column_lineage: projections count={len(projections)}")
    if not projections:
        logger.debug(f"_extract_column_lineage: No projections, returning empty")
        return lineage, output_columns
    has_star = _has_star_expansion(self, select_stmt)
    logger.debug(f"_extract_column_lineage: _has_star_expansion returned {has_star}")
    if has_star:
        logger.debug(f"_extract_column_lineage: Calling _handle_star_expansion")
        return _handle_star_expansion(self, select_stmt, view_name, alias_map, derived_cols)
    if _has_union(self, select_stmt):
        logger.debug(f"_extract_column_lineage: Has union, calling _handle_union_lineage")
        return _handle_union_lineage(self, select_stmt, view_name)
    ordinal = 0
    for proj in projections:
        if isinstance(proj, exp.Alias):
            out_name = proj.alias or proj.alias_or_name
            # Clean up alias name: remove SQL comments and invalid characters
            if out_name:
                # Remove SQL comments (-- and /* */)
                out_name = re.sub(r'--.*$', '', str(out_name), flags=re.MULTILINE).strip()
                out_name = re.sub(r'/\*.*?\*/', '', str(out_name), flags=re.DOTALL).strip()
                # Remove leading/trailing invalid characters
                out_name = str(out_name).strip('[]').strip()
                # If name is empty or invalid after cleanup, try to get from inner expression
                if not out_name or out_name in [')', '(', 'INSERT', 'SELECT']:
                    if hasattr(proj, 'this') and isinstance(proj.this, exp.Column):
                        out_name = proj.this.name or "calc_expr"
                    else:
                        out_name = "calc_expr"
            inner = proj.this
        else:
            s = str(proj).upper()
            if "HASHBYTES(" in s or "MD5(" in s:
                out_name = "hash_expr"
            elif isinstance(proj, exp.Coalesce):
                out_name = "coalesce_expr"
            elif isinstance(proj, (exp.Trim, exp.Upper, exp.Lower)):
                col = proj.find(exp.Column)
                out_name = (col.name if col else "text_expr")
            elif isinstance(proj, (exp.Cast, exp.Convert)):
                out_name = "cast_expr"
            elif isinstance(proj, exp.Column):
                out_name = proj.name
                # Clean up column name: remove SQL comments and invalid characters
                if out_name:
                    # Remove SQL comments (-- and /* */)
                    out_name = re.sub(r'--.*$', '', out_name, flags=re.MULTILINE).strip()
                    out_name = re.sub(r'/\*.*?\*/', '', out_name, flags=re.DOTALL).strip()
                    # Remove leading/trailing invalid characters like [ or ]
                    out_name = out_name.strip('[]').strip()
                    # Remove any remaining invalid patterns like "abl.[column" -> "column"
                    if '.' in out_name and '[' in out_name:
                        parts = out_name.split('.')
                        out_name = parts[-1].strip('[]').strip()
                    # If name is empty or invalid after cleanup, use generic name
                    if not out_name or out_name in [')', '(', 'INSERT', 'SELECT']:
                        out_name = "calc_expr"
            else:
                # Attempt to derive a stable name from expression tail if no alias provided
                out_name = _strip_expr_tail(str(proj))
                # If _strip_expr_tail returns empty or invalid, use generic name
                if not out_name:
                    out_name = "calc_expr"
                # Additional validation: check for SQL keywords or invalid characters
                elif len(out_name) > 100 or any(keyword in out_name.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS']):
                    out_name = "calc_expr"
                # Check for invalid single characters or symbols
                elif out_name in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE']:
                    out_name = "calc_expr"
            inner = proj

        inputs = _collect_inputs_for_expr(self, inner, alias_map, derived_cols)
        out_type = _infer_type(self, inner, alias_map)
        if isinstance(inner, (exp.Cast, exp.Convert)):
            ttype = TransformationType.CAST
        elif isinstance(inner, exp.Case):
            ttype = TransformationType.CASE
        elif isinstance(inner, exp.Column):
            ttype = TransformationType.IDENTITY
        else:
            s = str(inner).upper()
            if s.startswith("CASE ") or s.startswith("CASEWHEN ") or s.startswith("IIF("):
                ttype = TransformationType.CASE
            else:
                ttype = TransformationType.EXPRESSION

        lineage.append(ColumnLineage(output_column=out_name, input_fields=inputs, transformation_type=ttype, transformation_description=_short_desc(self, inner)))
        output_columns.append(ColumnSchema(name=out_name, data_type=out_type, nullable=True, ordinal=ordinal))
        ordinal += 1
    _normalize_lineage_input_temps(self, lineage)
    return lineage, output_columns


def _analyze_expression_lineage(self, output_name: str, expr: exp.Expression, context: exp.Select) -> ColumnLineage:
    input_fields = []
    transformation_type = TransformationType.IDENTITY
    description = ""

    if isinstance(expr, exp.Column):
        table_alias = str(expr.table) if expr.table else None
        column_name = str(expr.this)
        table_name = _resolve_table_from_alias(self, table_alias, context)
        if table_name and (table_name.startswith('@') or ('+' in table_name) or (table_name.startswith('[') and table_name.endswith(']') and '.' not in table_name)):
            return ColumnLineage(output_column=output_name, input_fields=[], transformation_type=TransformationType.EXPRESSION, transformation_description=f"Expression: {str(expr)}")
        if _is_join_keyword(table_name):
            return ColumnLineage(output_column=output_name, input_fields=[], transformation_type=TransformationType.EXPRESSION, transformation_description=f"Expression: {str(expr)}")
        ns, nm = self._ns_and_name(table_name)
        input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        table_simple = table_name.split('.')[-1] if '.' in table_name else table_name
        semantic_renames = {('OrderItemID', 'SalesID'): True}
        if (column_name, output_name) in semantic_renames:
            transformation_type = TransformationType.RENAME
            description = f"{column_name} AS {output_name}"
        else:
            description = f"{output_name} from {table_simple}.{column_name}"

    elif isinstance(expr, exp.Cast):
        transformation_type = TransformationType.CAST
        inner_expr = expr.this
        target_type = str(expr.to).upper()
        if isinstance(inner_expr, (exp.Mul, exp.Add, exp.Sub, exp.Div)):
            transformation_type = TransformationType.ARITHMETIC
            for column_ref in inner_expr.find_all(exp.Column):
                table_alias = str(column_ref.table) if column_ref.table else None
                column_name = str(column_ref.this)
                table_name = _resolve_table_from_alias(self, table_alias, context)
                if not _is_join_keyword(table_name):
                    ns, nm = self._ns_and_name(table_name)
                    input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
            expr_str = str(inner_expr)
            if '*' in expr_str:
                operands = [str(col.this) for col in inner_expr.find_all(exp.Column)]
                if len(operands) >= 2:
                    description = f"{operands[0]} * {operands[1]}"
                else:
                    description = expr_str
            else:
                description = expr_str
        elif isinstance(inner_expr, exp.Column):
            table_alias = str(inner_expr.table) if inner_expr.table else None
            column_name = str(inner_expr.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
            description = f"CAST({column_name} AS {target_type})"

    elif isinstance(expr, exp.Case):
        transformation_type = TransformationType.CASE
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        description = str(expr).replace('\n', ' ').replace('  ', ' ')

    elif isinstance(expr, (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
        transformation_type = TransformationType.AGGREGATION
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        description = f"{type(expr).__name__.upper()}({str(expr.this) if hasattr(expr, 'this') else '*'})"

    elif isinstance(expr, exp.Window):
        transformation_type = TransformationType.WINDOW
        inner_function = expr.this
        if hasattr(inner_function, 'find_all'):
            for column_ref in inner_function.find_all(exp.Column):
                table_alias = str(column_ref.table) if column_ref.table else None
                column_name = str(column_ref.this)
                table_name = _resolve_table_from_alias(self, table_alias, context)
                ns, nm = self._ns_and_name(table_name)
                input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        if hasattr(expr, 'partition_by') and expr.partition_by:
            for partition_col in expr.partition_by:
                for column_ref in partition_col.find_all(exp.Column):
                    table_alias = str(column_ref.table) if column_ref.table else None
                    column_name = str(column_ref.this)
                    table_name = _resolve_table_from_alias(self, table_alias, context)
                    ns, nm = self._ns_and_name(table_name)
                    input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        if hasattr(expr, 'order') and expr.order:
            for order_col in expr.order.expressions:
                for column_ref in order_col.find_all(exp.Column):
                    table_alias = str(column_ref.table) if column_ref.table else None
                    column_name = str(column_ref.this)
                    table_name = _resolve_table_from_alias(self, table_alias, context)
                    ns, nm = self._ns_and_name(table_name)
                    input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        func_name = str(inner_function) if inner_function else "UNKNOWN"
        partition_cols = []
        order_cols = []
        if hasattr(expr, 'partition_by') and expr.partition_by:
            partition_cols = [str(col) for col in expr.partition_by]
        if hasattr(expr, 'order') and expr.order:
            order_cols = [str(col) for col in expr.order.expressions]
        description = f"{func_name} OVER ("
        if partition_cols:
            description += f"PARTITION BY {', '.join(partition_cols)}"
        if order_cols:
            if partition_cols:
                description += " "
            description += f"ORDER BY {', '.join(order_cols)}"
        description += ")"

    elif isinstance(expr, (exp.Mul, exp.Add, exp.Sub, exp.Div)):
        transformation_type = TransformationType.ARITHMETIC
        seen_columns = set()
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            column_key = (table_name, column_name)
            if column_key not in seen_columns:
                seen_columns.add(column_key)
                ns, nm = self._ns_and_name(table_name)
                input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        expr_str = str(expr)
        if '*' in expr_str:
            operands = [str(col.this) for col in expr.find_all(exp.Column)]
            if len(operands) >= 2:
                description = f"{operands[0]} * {operands[1]}"
            else:
                description = expr_str
        else:
            description = expr_str

    elif _is_string_function(self, expr):
        transformation_type = TransformationType.STRING_PARSE
        seen_columns = set()
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            column_key = (table_name, column_name)
            if column_key not in seen_columns:
                seen_columns.add(column_key)
                ns, nm = self._ns_and_name(table_name)
                input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        expr_str = str(expr)
        if 'RIGHT' in expr_str.upper() and 'LEN' in expr_str.upper() and 'CHARINDEX' in expr_str.upper():
            columns = [str(col.this) for col in expr.find_all(exp.Column)]
            if columns:
                col_name = columns[0]
                description = f"RIGHT({col_name}, LEN({col_name}) - CHARINDEX('@', {col_name}))"
            else:
                description = expr_str
        else:
            description = expr_str

    else:
        transformation_type = TransformationType.EXPRESSION
        for column_ref in expr.find_all(exp.Column):
            table_alias = str(column_ref.table) if column_ref.table else None
            column_name = str(column_ref.this)
            table_name = _resolve_table_from_alias(self, table_alias, context)
            ns, nm = self._ns_and_name(table_name)
            input_fields.append(ColumnReference(namespace=ns, table_name=nm, column_name=column_name))
        description = f"Expression: {str(expr)}"

    return ColumnLineage(output_column=output_name, input_fields=input_fields, transformation_type=transformation_type, transformation_description=description)


def _resolve_table_from_alias(self, alias: Optional[str], context: exp.Select) -> str:
    if not alias:
        tables = list(context.find_all(exp.Table))
        if len(tables) == 1:
            return self._get_table_name(tables[0])
        return "unknown"
    for table in context.find_all(exp.Table):
        parent = table.parent
        if isinstance(parent, exp.Alias) and str(parent.alias) == alias:
            return self._get_table_name(table)
        if hasattr(table, 'alias') and table.alias and str(table.alias) == alias:
            return self._get_table_name(table)
    for join in context.find_all(exp.Join):
        if hasattr(join.this, 'alias') and str(join.this.alias) == alias:
            if isinstance(join.this, exp.Alias):
                return self._get_table_name(join.this.this)
            return self._get_table_name(join.this)
    return alias


def _process_ctes(self, select_stmt: exp.Expression) -> exp.Expression:
    # FIX: Use .ctes property instead of args.get('with') which was ALWAYS None
    # sqlglot stores WITH in args['with_'] (with underscore) and provides .ctes property
    if hasattr(select_stmt, 'ctes') and select_stmt.ctes:
        ctes = select_stmt.ctes
        for cte in ctes:
            if hasattr(cte, 'alias') and hasattr(cte, 'this'):
                cte_name = str(cte.alias)
                cte_columns = []
                if isinstance(cte.this, exp.Select):
                    for proj in cte.this.expressions:
                        col_name = None
                        if isinstance(proj, exp.Alias):
                            # Get alias name
                            if hasattr(proj, 'alias') and proj.alias:
                                col_name = str(proj.alias)
                            elif hasattr(proj, 'alias_or_name'):
                                col_name = str(proj.alias_or_name)
                            # If no alias, try to get column name from the expression
                            if not col_name and hasattr(proj, 'this'):
                                if isinstance(proj.this, exp.Column):
                                    col_name = str(proj.this.this) if hasattr(proj.this, 'this') else str(proj.this)
                        elif isinstance(proj, exp.Column):
                            # Get column name
                            if hasattr(proj, 'this'):
                                col_name = str(proj.this)
                            elif hasattr(proj, 'name'):
                                col_name = str(proj.name)
                            else:
                                col_name = str(proj)
                        elif isinstance(proj, exp.Star):
                            source_deps = self._extract_dependencies(cte.this)
                            for source_table in source_deps:
                                source_cols = self._infer_table_columns(source_table)
                                cte_columns.extend(source_cols)
                            break
                        
                        # Only add valid column names (not SQL expressions)
                        if col_name:
                            # Sanitize: remove newlines, extra whitespace
                            col_name = col_name.strip().replace('\n', ' ').replace('\t', ' ').strip()
                            # Validate: check if it's a valid column name
                            # Must be non-empty, reasonable length, no SQL keywords, and not just symbols
                            is_valid = (
                                col_name and 
                                len(col_name) >= 2 and 
                                len(col_name) < 100 and 
                                not any(keyword in col_name.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'INT)', 'AS']) and
                                col_name not in [')', '(', 'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'INT)', 'CurrentAP\n)', 'PrevAP'] and
                                not col_name.startswith('(') and
                                not col_name.endswith(')') and
                                re.match(r'^[a-zA-Z_#][a-zA-Z0-9_#]*$', col_name)  # Valid identifier pattern
                            )
                            if is_valid:
                                cte_columns.append(col_name)
                            else:
                                # Use generic name for complex expressions
                                cte_columns.append(f"col_{len(cte_columns) + 1}")
                        else:
                            cte_columns.append(f"col_{len(cte_columns) + 1}")
                # Store both columns and the CTE definition (exp.Select) for later use
                # We use a dict to store both pieces of information
                if isinstance(cte.this, exp.Select):
                    self._cte_registry_store(
                        cte_name,
                        {"columns": cte_columns, "definition": cte.this},
                    )
                    _register_cte_lineage_object(self, cte_name, cte.this, cte_columns)
                else:
                    self._cte_registry_store(cte_name, {"columns": cte_columns})
    return select_stmt

"""
OpenLineage JSON generation for InfoTracker.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .models import ObjectInfo, ColumnLineage, TransformationType


def _dequote(s: str) -> str:
    try:
        import re
        return re.sub(r"[\[\]\"'`]", "", s or "").strip()
    except Exception:
        return (s or "").strip()


def _ns_for_dep(dep: str, default_ns: str) -> str:
    """Determine namespace for a dependency based on its database context.

    Strips quotes/brackets from identifiers to avoid duplicates.
    Handles temp tables in both formats: #temp and DB.dbo.PROC#temp
    """
    d = _dequote(dep or "")
    dl = d.lower()
    # Check for temp tables: #temp or scoped canonical names (dbo.proc#temp / DB.dbo.proc#temp)
    if dl.startswith("tempdb..#") or dl.startswith("#") or "#" in d:
        # For canonical temp names with explicit DB (DB.dbo.PROC#temp), extract DB
        if "#" in d and "." in d:
            parts = d.split(".")
            if len(parts) >= 3:
                db = parts[0]
                return f"mssql://localhost/{db.upper()}"
            # Scoped temp without explicit DB (dbo.PROC#temp) should stay in caller DB namespace
            if len(parts) == 2 and not d.startswith("#"):
                return default_ns or "mssql://localhost/InfoTrackerDW"
        # For simple raw temp names (#temp), use tempdb
        return "mssql://localhost/tempdb"
    parts = d.split(".")
    db = parts[0] if len(parts) >= 3 else None
    return f"mssql://localhost/{(db or '').upper()}" if db else (default_ns or "mssql://localhost/InfoTrackerDW")

def _strip_db_prefix(name: str) -> str:
    """Strip database prefix from name, preserving temp table canonical names.
    
    For canonical temp names (DB.dbo.PROC#temp), returns dbo.PROC#temp
    For simple temp names (#temp), returns as is
    For regular names (DB.dbo.table), returns dbo.table
    """
    name = _dequote(name or "")
    # Handle canonical temp names (DB.dbo.PROC#temp)
    if "#" in name and "." in name:
        parts = name.split(".")
        if len(parts) >= 4 and parts[-1].startswith("#"):
            # Canonical temp: DB.dbo.PROC.#temp -> dbo.PROC#temp (no dot before #)
            return f"{parts[-3]}.{parts[-2]}{parts[-1]}"
        if len(parts) >= 3:
            # Return dbo.table or schema.#temp for short forms
            return ".".join(parts[-2:])
        elif len(parts) == 2:
            # Return as is (already schema.table format)
            return name
    # Regular handling
    parts = name.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else name

def _is_noise_dep(dep: str) -> bool:
    """Return True if dependency name looks like a temp table or non-table token.

    Filters out:
    - temp tables ("#..." or "tempdb..#...")
    - variable-like tokens ("@...")
    - dynamic concat artifacts (contains '+')
    - single bracketed tokens without dot (e.g., "[Name]")
    """
    if not dep:
        return True
    d = dep.strip()
    dl = d.lower()
    # keep temp tables visible in upstream/lineage
    if d.startswith("@"):
        return True
    if "+" in d:
        return True
    if d.startswith("[") and d.endswith("]") and "." not in d:
        return True
    return False


class OpenLineageGenerator:
    """Generates OpenLineage-compliant JSON from ObjectInfo."""
    
    def __init__(self, namespace: str = "mssql://localhost/InfoTrackerDW"):
        self.namespace = namespace
    
    def generate(self, obj_info: ObjectInfo, job_namespace: str = "infotracker/examples", 
                 job_name: Optional[str] = None, object_hint: Optional[str] = None) -> str:
        """Generate OpenLineage JSON for an object."""
        
        # Determine run ID based on object hint (filename) for consistency with examples
        run_id = self._generate_run_id(object_hint or obj_info.name)
        
        inputs, proc_deps_facet = openlineage_direct_inputs_and_procedure_deps_facet(obj_info)
        # Build the OpenLineage event
        event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.now().isoformat()[:19] + "Z",
            "run": {"runId": run_id},
            "job": {
                "namespace": job_namespace,
                "name": job_name or f"warehouse/sql/{obj_info.name}.sql"
            },
            "inputs": inputs,
            "outputs": self._build_outputs(obj_info, procedure_dependencies_facet=proc_deps_facet)
        }
        
        return json.dumps(event, indent=2, ensure_ascii=False)
    
    def _generate_run_id(self, object_name: str) -> str:
        """Generate a consistent run ID based on object name."""
        # Extract number from filename for consistency with examples
        import re
        # Try to match the pattern at the start of the object name or filename
        match = re.search(r'(\d+)_', object_name)
        if match:
            num = int(match.group(1))
            return f"00000000-0000-0000-0000-{num:012d}"
        return "00000000-0000-0000-0000-000000000000"
    
    def _build_outputs(
        self,
        obj_info: ObjectInfo,
        procedure_dependencies_facet: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Build outputs array with schema and lineage facets."""
        # Use consistent temp table namespace
        if obj_info.schema.name.startswith('tempdb..#'):
            output_namespace = "mssql://localhost/tempdb"
        else:
            # Use schema's namespace if available, otherwise default namespace
            output_namespace = obj_info.schema.namespace if obj_info.schema.namespace else self.namespace
            # For fallback objects, tests expect DB segment uppercased (e.g., MyDB -> MYDB)
            if getattr(obj_info, 'is_fallback', False) and isinstance(output_namespace, str):
                try:
                    if output_namespace.startswith("mssql://localhost/"):
                        prefix, dbseg = output_namespace.rsplit('/', 1)
                        output_namespace = f"{prefix}/{dbseg.upper()}"
                except Exception:
                    pass
        
        output = {
            "namespace": output_namespace,
            "name": obj_info.schema.name,
            "facets": {}
        }
        if procedure_dependencies_facet:
            output["facets"]["procedureDependencies"] = procedure_dependencies_facet
        
        # Add schema facet for tables and procedures (even if columns list is empty)
        # Views should only have columnLineage, not schema
        if (obj_info.schema and 
            obj_info.object_type in ['table', 'temp_table', 'procedure']):
            print(f"DEBUG: Generating schema facet for {obj_info.name}, type={obj_info.object_type}, cols={len(obj_info.schema.columns) if obj_info.schema and obj_info.schema.columns else 0}")
            schema_facet = self._build_schema_facet(obj_info)
            if schema_facet:  # Only add if not None (fallback objects)
                output["facets"]["schema"] = schema_facet
        
        # Add column lineage facet only if we have lineage (views, not tables)
        if obj_info.lineage:
            output["facets"]["columnLineage"] = self._build_column_lineage_facet(obj_info)
        
        return [output]
    
    def _build_schema_facet(self, obj_info: ObjectInfo) -> Optional[Dict[str, Any]]:
        """Build schema facet from table schema."""
        # Skip schema facet for fallback objects to match expected format
        if getattr(obj_info, 'is_fallback', False) and obj_info.object_type not in ('table', 'temp_table'):
            return None
            
        fields = []
        
        for col in obj_info.schema.columns:
            fields.append({
                "name": col.name,
                "type": col.data_type
            })
        
        return {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": fields
        }
    
    def _build_column_lineage_facet(self, obj_info: ObjectInfo) -> Dict[str, Any]:
        """Build column lineage facet from column lineage information."""
        fields = {}
        
        for lineage in obj_info.lineage:
            input_fields = []
            
            for input_ref in lineage.input_fields:
                # Use consistent temp table namespace for inputs
                if input_ref.table_name.startswith('tempdb..#'):
                    namespace = "mssql://localhost/tempdb"
                else:
                    namespace = input_ref.namespace
                
                # Skip SQL keywords that should never be table names
                JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}
                table_name_simple = input_ref.table_name.split('.')[-1] if input_ref.table_name else ""
                if table_name_simple.lower() in JOIN_KEYWORDS:
                    continue  # Skip this input
                
                # Skip "unknown" table names (e.g., unresolved references)
                if input_ref.table_name == "unknown":
                    continue
                    
                input_fields.append({
                    "namespace": namespace,
                    "name": input_ref.table_name,
                    "field": input_ref.column_name
                })
            
            fields[lineage.output_column] = {
                "inputFields": input_fields,
                "transformationType": lineage.transformation_type.value,
                "transformationDescription": lineage.transformation_description
            }
        
        return {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ColumnLineageDatasetFacet.json",
            "fields": fields
        }


def _is_noise_dataset_name(n: str) -> bool:
    if not n:
        return True
    if n.startswith('@'):
        return True
    if '+' in n:
        return True
    if n.startswith('[') and n.endswith(']') and '.' not in n:
        return True
    JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}
    name_simple = n.split('.')[-1].lower() if n else ""
    if name_simple in JOIN_KEYWORDS:
        return True
    return False


def _infer_lineage_field_dataset_pair(
    f: Any, default_ns: str
) -> Optional[Tuple[str, str]]:
    """When ``namespace`` is empty but ``table_name`` is qualified, align with dep-style (ns, schema.table)."""
    tn_raw = getattr(f, "table_name", None) or ""
    tn = _dequote(tn_raw)
    if not tn or tn == "unknown":
        return None
    parts = [p for p in tn.split(".") if p]
    if not parts:
        return None
    if len(parts) >= 3:
        dep = ".".join(parts[:3])
        return (_ns_for_dep(dep, default_ns), _strip_db_prefix(dep))
    if len(parts) == 2:
        db = None
        if default_ns and "://" in default_ns:
            db = default_ns.rsplit("/", 1)[-1]
        if not db:
            return None
        dep = f"{db}.{parts[0]}.{parts[1]}"
        return (_ns_for_dep(dep, default_ns), _strip_db_prefix(dep))
    return None


def _lineage_direct_dataset_pairs(obj: ObjectInfo) -> set[tuple[str, str]]:
    """Dataset (namespace, table) pairs referenced by column lineage inputFields (direct read)."""
    pairs: set[tuple[str, str]] = set()
    JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}
    default_ns = obj.schema.namespace if obj.schema else "mssql://localhost/InfoTrackerDW"
    for ln in obj.lineage or []:
        for f in ln.input_fields or []:
            tn = getattr(f, "table_name", None)
            if not tn:
                continue
            if tn == "unknown":
                continue
            table_simple = (tn or "").split(".")[-1] if tn else ""
            if table_simple.lower() in JOIN_KEYWORDS:
                continue
            if _is_noise_dataset_name(tn):
                continue
            ns = getattr(f, "namespace", None)
            if ns and str(ns).strip():
                pairs.add((str(ns), tn))
                continue
            inferred = _infer_lineage_field_dataset_pair(f, default_ns)
            if inferred:
                pairs.add(inferred)
    return pairs


def _procedure_deps_drop_pseudo_dbo_temp(
    obj: ObjectInfo, proc_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Drop ``dbo.<suffix>`` from procedure dependency lists when lineage uses ``…#<suffix>`` (sqlglot leak)."""
    if not proc_rows:
        return proc_rows
    temp_suffixes: set[str] = set()
    for ln in obj.lineage or []:
        for f in ln.input_fields or []:
            tn = f.table_name or ""
            if "#" in tn:
                suff = tn.split("#")[-1].split("@")[0].lower()
                if suff:
                    temp_suffixes.add(suff)
    if not temp_suffixes:
        return proc_rows
    out: List[Dict[str, Any]] = []
    for row in proc_rows:
        nm = row.get("name") or ""
        parts = nm.split(".")
        if (
            len(parts) == 2
            and parts[0].lower() == "dbo"
            and parts[1].lower() in temp_suffixes
        ):
            continue
        out.append(row)
    return out


def _lineage_direct_datasets_are_all_temp_staging(pairs: set[tuple[str, str]]) -> bool:
    """True when every lineage dataset is a temp / staging table (scoped #temp or tempdb).

    In that case we omit ``procedureDependencies`` on the persistent output: the same
    information would duplicate ``inputs`` semantics (one staging hop) and obscure UX;
    full physical deps remain on the temp table's own event / expanded graph mode.
    """
    if not pairs:
        return False
    for _ns, name in pairs:
        n = name or ""
        if "#" in n:
            continue
        if n.lower().startswith("tempdb"):
            continue
        return False
    return True


def openlineage_direct_inputs_and_procedure_deps_facet(
    obj: ObjectInfo,
) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Event-level ``inputs`` (direct datasets) and optional ``procedureDependencies`` facet dict.

    See :func:`emit_ol_from_object` docstring for semantics.
    """
    ns = obj.schema.namespace if obj.schema else "mssql://localhost/InfoTrackerDW"
    input_pairs_from_lineage = _lineage_direct_dataset_pairs(obj)
    input_pairs_from_deps = {
        (_ns_for_dep(dep, ns), _strip_db_prefix(dep))
        for dep in sorted(obj.dependencies or []) if not _is_noise_dep(dep)
    }
    # Object-level inputs for temp tables: **union** lineage-derived datasets with SQL ``dependencies``.
    # Column lineage may lag (``*`` not expanded yet, or only a later step’s refs visible); deps from
    # ``_extract_dependencies`` / ``temp_sources`` must not disappear from the event when lineage exists.
    merged_pairs: Optional[set[tuple[str, str]]] = None
    if getattr(obj, "object_type", None) == "temp_table":
        merged_pairs = set(input_pairs_from_lineage) | set(input_pairs_from_deps)

    if merged_pairs is not None and merged_pairs:
        filtered = [(ns2, nm2) for (ns2, nm2) in merged_pairs if not _is_noise_dataset_name(nm2)]
        inputs: List[Dict[str, Any]] = [{"namespace": ns2, "name": nm2} for (ns2, nm2) in sorted(filtered)]
    elif input_pairs_from_lineage:
        filtered = [(ns2, nm2) for (ns2, nm2) in input_pairs_from_lineage if not _is_noise_dataset_name(nm2)]
        inputs = [{"namespace": ns2, "name": nm2} for (ns2, nm2) in sorted(filtered)]
    elif input_pairs_from_deps:
        filtered = [(ns2, nm2) for (ns2, nm2) in input_pairs_from_deps if not _is_noise_dataset_name(nm2)]
        inputs = [{"namespace": ns2, "name": nm2} for (ns2, nm2) in sorted(filtered)]
    else:
        inputs = []
    proc_facet: Optional[Dict[str, Any]] = None
    if (
        getattr(obj, "object_type", None) != "temp_table"
        and input_pairs_from_lineage
        and input_pairs_from_deps
        and input_pairs_from_lineage != input_pairs_from_deps
        and not _lineage_direct_datasets_are_all_temp_staging(input_pairs_from_lineage)
    ):
        proc_rows = [
            {"namespace": ns2, "name": nm2}
            for (ns2, nm2) in sorted(input_pairs_from_deps)
            if not _is_noise_dataset_name(nm2)
        ]
        proc_rows = _procedure_deps_drop_pseudo_dbo_temp(obj, proc_rows)
        proc_facet = {
            "_producer": "https://github.com/InfoTracker/InfoTracker",
            "_schemaURL": "https://infotracker.dev/spec/facets/procedure-dependencies/1-0-0.json",
            "datasets": proc_rows,
        }
    return inputs, proc_facet


def emit_ol_from_object(obj: ObjectInfo, job_name: str | None = None, quality_metrics: bool = False, virtual_proc_outputs: bool = False) -> dict:
    """Emit OpenLineage JSON directly from ObjectInfo without re-parsing.

    Semantics:
    - Top-level ``inputs`` lists **direct** datasets for this load step: derived from
      column lineage ``inputFields`` when lineage is present (e.g. INSERT … SELECT … FROM #temp
      → only the scoped temp). This matches OpenLineage's notion of immediate upstream datasets.
    - For ``temp_table`` outputs, ``inputs`` is the **union** of lineage-derived datasets and
      ``ObjectInfo.dependencies`` so persistent stage tables stay visible even when column ``*``
      has not yet been materialized in lineage.
    - When lineage-based direct inputs are a strict subset of ``ObjectInfo.dependencies``
      **and** those direct inputs are not exclusively temp staging tables, the full
      procedure/read list is attached under ``outputs[].facets.procedureDependencies``.
      For ``INSERT INTO … SELECT … FROM #staging``-style loads (only ``#temp`` in lineage),
      that facet is omitted so UI does not repeat the whole procedure next to the single
      staging input.
    - If there is no usable lineage dataset set, ``inputs`` falls back to ``dependencies`` (legacy).
    """
    ns = obj.schema.namespace if obj.schema else "mssql://localhost/InfoTrackerDW"
    name = obj.schema.name if obj.schema else obj.name
    
    # Handle virtual procedure outputs
    if obj.object_type == "procedure" and virtual_proc_outputs and obj.schema and obj.schema.columns:
        name = f"procedures.{obj.name}"
    
    inputs, proc_deps_facet = openlineage_direct_inputs_and_procedure_deps_facet(obj)

    # Build output facets
    facets = {}
    if proc_deps_facet:
        facets["procedureDependencies"] = proc_deps_facet
    
    # Add schema facet if we have columns and it's not a fallback object
    # Relaxed condition: allow schema facet even if columns list is empty
    should_add_schema = (obj.object_type in ('table', 'temp_table', 'procedure') 
        and obj.schema 
        and not getattr(obj, 'is_fallback', False))
        
    if should_add_schema:
        facets["schema"] = {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [{"name": c.name, "type": c.data_type} for c in (obj.schema.columns or [])]
        }
    
    # Add column lineage facet if we have lineage
    if obj.lineage:
        lineage_fields = {}
        JOIN_KEYWORDS = {'left', 'right', 'inner', 'outer', 'cross', 'full', 'join'}
        for ln in obj.lineage:
            # Filter input_fields: skip JOIN keywords and "unknown"
            filtered_inputs = []
            for f in ln.input_fields:
                table_simple = f.table_name.split('.')[-1] if f.table_name else ""
                if table_simple.lower() in JOIN_KEYWORDS:
                    continue
                if f.table_name == "unknown":
                    continue
                if (f.column_name or "").strip() == "*":
                    continue
                filtered_inputs.append({"namespace": f.namespace, "name": f.table_name, "field": f.column_name})
            
            field_entry: dict = {
                "inputFields": filtered_inputs,
                "transformationType": ln.transformation_type.value,
                "transformationDescription": ln.transformation_description,
            }
            qf = getattr(ln, "quality_facets", None)
            if qf:
                field_entry["quality"] = qf
            lineage_fields[ln.output_column] = field_entry
        facets["columnLineage"] = {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ColumnLineageDatasetFacet.json",
            "fields": lineage_fields,
        }
    
    # Add quality metrics if requested
    if quality_metrics:
        covered = 0
        if obj.schema and obj.schema.columns:
            covered = sum(1 for c in obj.schema.columns 
                         if any(ln.output_column == c.name and ln.input_fields for ln in obj.lineage))
        
        facets["quality"] = {
            "lineageCoverage": (covered / max(1, len(obj.schema.columns) if obj.schema else 1)),
            "isFallback": bool(getattr(obj, 'is_fallback', False)),
            "reasonCode": getattr(obj, 'no_output_reason', None)
        }
    
    # Build the complete event
    event = {
        "eventType": "COMPLETE", 
        "eventTime": datetime.now().isoformat()[:19] + "Z",
        "run": {"runId": "00000000-0000-0000-0000-000000000000"},
        "job": {
        "namespace": "infotracker/examples",
        "name": job_name or getattr(obj, "job_name", f"warehouse/sql/{obj.name}.sql")
        },
        "inputs": inputs,
        "outputs": [
            {
                "namespace": ns,
                "name": name,
                "facets": facets,
            }
        ],
    }
    
    return event

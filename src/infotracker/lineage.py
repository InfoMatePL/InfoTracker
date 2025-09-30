"""
OpenLineage JSON generation for InfoTracker.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List, Any, Optional

from .models import ObjectInfo, ColumnLineage, TransformationType


def _ns_for_dep(dep: str, default_ns: str) -> str:
    """Determine namespace for a dependency based on its database context."""
    d = (dep or "").strip()
    dl = d.lower()
    if dl.startswith("tempdb..#") or dl.startswith("#"):
        return "mssql://localhost/tempdb"
    parts = d.split(".")
    db = parts[0] if len(parts) >= 3 else None
    return f"mssql://localhost/{db}" if db else (default_ns or "mssql://localhost/InfoTrackerDW")

def _strip_db_prefix(name: str) -> str:
    parts = (name or "").split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else (name or "")


class OpenLineageGenerator:
    """Generates OpenLineage-compliant JSON from ObjectInfo."""
    
    def __init__(self, namespace: str = "mssql://localhost/InfoTrackerDW"):
        self.namespace = namespace
    
    def generate(self, obj_info: ObjectInfo, job_namespace: str = "infotracker/examples", 
                 job_name: Optional[str] = None, object_hint: Optional[str] = None) -> str:
        """Generate OpenLineage JSON for an object."""
        
        # Determine run ID based on object hint (filename) for consistency with examples
        run_id = self._generate_run_id(object_hint or obj_info.name)
        
        # Build the OpenLineage event
        event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.now().isoformat()[:19] + "Z",
            "run": {"runId": run_id},
            "job": {
                "namespace": job_namespace,
                "name": job_name or f"warehouse/sql/{obj_info.name}.sql"
            },
            "inputs": self._build_inputs(obj_info),
            "outputs": self._build_outputs(obj_info)
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
    
    def _build_inputs(self, obj_info: ObjectInfo) -> List[Dict[str, Any]]:
        """Build inputs array from object dependencies."""
        inputs = []
        for dep_name in sorted(obj_info.dependencies):
             # tempdb: stały namespace
             if dep_name.startswith('tempdb..#'):
                 namespace = "mssql://localhost/tempdb"
             else:
                 parts = dep_name.split('.')
                 db = parts[0] if len(parts) >= 3 else None
                 namespace = f"mssql://localhost/{db}" if db else self.namespace
             # w name trzymaj schema.table (bez prefiksu DB)
             name = ".".join(dep_name.split(".")[-2:]) if "." in dep_name else dep_name
             inputs.append({"namespace": namespace, "name": name})

        
        return inputs
    
    def _build_outputs(self, obj_info: ObjectInfo) -> List[Dict[str, Any]]:
        """Build outputs array with schema and lineage facets."""
        # Use consistent temp table namespace
        if obj_info.schema.name.startswith('tempdb..#'):
            output_namespace = "mssql://localhost/tempdb"
        else:
            # Use schema's namespace if available, otherwise default namespace
            output_namespace = obj_info.schema.namespace if obj_info.schema.namespace else self.namespace
        
        output = {
            "namespace": output_namespace,
            "name": obj_info.schema.name,
            "facets": {}
        }
        
        # Add schema facet for tables and procedures with columns
        # Views should only have columnLineage, not schema
        if (obj_info.schema and obj_info.schema.columns and 
            obj_info.object_type in ['table', 'temp_table', 'procedure']):
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


def emit_ol_from_object(obj: ObjectInfo, job_name: str | None = None, quality_metrics: bool = False, virtual_proc_outputs: bool = False) -> dict:
    """Emit OpenLineage JSON directly from ObjectInfo without re-parsing."""
    ns = obj.schema.namespace if obj.schema else "mssql://localhost/InfoTrackerDW"
    name = obj.schema.name if obj.schema else obj.name
    
    # Handle virtual procedure outputs
    if obj.object_type == "procedure" and virtual_proc_outputs and obj.schema and obj.schema.columns:
        name = f"procedures.{obj.name}"
    
    # Build inputs from dependencies with per-dependency namespaces
    if obj.lineage:
        input_pairs = {
            (f.namespace, f.table_name)
            for ln in obj.lineage
            for f in ln.input_fields
            if getattr(f, "namespace", None) and getattr(f, "table_name", None)
        }
        if input_pairs:
            inputs = [{"namespace": ns2, "name": nm2} for (ns2, nm2) in sorted(input_pairs)]
        else:
            inputs = [{"namespace": _ns_for_dep(dep, ns), "name": _strip_db_prefix(dep)}
                      for dep in sorted(obj.dependencies)]
    else:
        inputs = [{"namespace": _ns_for_dep(dep, ns), "name": _strip_db_prefix(dep)}
                  for dep in sorted(obj.dependencies)]

    # Build output facets
    facets = {}
    
    # Add schema facet if we have columns and it's not a fallback object
    if (obj.object_type in ('table', 'temp_table', 'procedure') 
        and obj.schema and obj.schema.columns 
        and not getattr(obj, 'is_fallback', False)):
        facets["schema"] = {
            "_producer": "https://github.com/OpenLineage/OpenLineage",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [{"name": c.name, "type": c.data_type} for c in obj.schema.columns]
        }
    
    # Add column lineage facet if we have lineage
    if obj.lineage:
        lineage_fields = {}
        for ln in obj.lineage:
            lineage_fields[ln.output_column] = {
                "inputFields": [
                    {"namespace": f.namespace, "name": f.table_name, "field": f.column_name}
                    for f in ln.input_fields
                ],
                "transformationType": ln.transformation_type.value,
                "transformationDescription": ln.transformation_description,
            }
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

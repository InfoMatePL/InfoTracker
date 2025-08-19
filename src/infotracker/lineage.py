"""
OpenLineage JSON generation for InfoTracker.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List, Any, Optional

from .models import ObjectInfo, ColumnLineage, TransformationType


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
            "eventTime": "2025-01-01T00:00:00Z",  # Fixed timestamp for consistency
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
            inputs.append({
                "namespace": self.namespace,
                "name": dep_name
            })
        
        return inputs
    
    def _build_outputs(self, obj_info: ObjectInfo) -> List[Dict[str, Any]]:
        """Build outputs array with schema and lineage facets."""
        # Use schema's namespace if available, otherwise default namespace
        output_namespace = obj_info.schema.namespace if obj_info.schema.namespace else self.namespace
        
        output = {
            "namespace": output_namespace,
            "name": obj_info.schema.name,
            "facets": {}
        }
        
        # Add schema facet for all objects with known columns (tables, views, functions, procedures)
        if obj_info.schema and obj_info.schema.columns:
            output["facets"]["schema"] = self._build_schema_facet(obj_info)
        
        # Add column lineage facet only if we have lineage (views, not tables)
        if obj_info.lineage:
            output["facets"]["columnLineage"] = self._build_column_lineage_facet(obj_info)
        
        return [output]
    
    def _build_schema_facet(self, obj_info: ObjectInfo) -> Dict[str, Any]:
        """Build schema facet from table schema."""
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
                input_fields.append({
                    "namespace": input_ref.namespace,
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

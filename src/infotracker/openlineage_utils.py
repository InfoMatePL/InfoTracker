"""
Utilities for working with OpenLineage JSON artifacts.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Any, Optional

from .models import ObjectInfo, ColumnSchema, TableSchema, ColumnLineage, ColumnReference, TransformationType


class OpenLineageLoader:
    """Loads OpenLineage JSON artifacts from directories."""
    
    @classmethod
    def load_dir(cls, directory: Path) -> List[Dict[str, Any]]:
        """Load all OpenLineage JSON files from a directory."""
        artifacts = []
        
        if not directory.exists():
            return artifacts
            
        for json_file in directory.glob("*.json"):
            if json_file.name == "column_graph.json":
                continue  # Skip column graph file
                
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    artifact = json.load(f)
                    artifacts.append(artifact)
            except Exception as e:
                # Log warning but continue
                import logging
                logging.warning(f"Failed to load {json_file}: {e}")
                
        return artifacts


class OLMapper:
    """Maps OpenLineage artifacts to ObjectInfo instances."""
    
    @classmethod
    def to_object_infos(cls, artifacts: List[Dict[str, Any]]) -> List[ObjectInfo]:
        """Convert OpenLineage artifacts to ObjectInfo instances."""
        objects = []
        
        for artifact in artifacts:
            try:
                obj_info = cls._artifact_to_object_info(artifact)
                if obj_info:
                    objects.append(obj_info)
            except Exception as e:
                # Log warning but continue
                import logging
                logging.warning(f"Failed to convert artifact to ObjectInfo: {e}")
                
        return objects
    
    @classmethod
    def _artifact_to_object_info(cls, artifact: Dict[str, Any]) -> Optional[ObjectInfo]:
        """Convert a single OpenLineage artifact to ObjectInfo."""
        outputs = artifact.get("outputs", [])
        if not outputs:
            return None
            
        output = outputs[0]  # Take first output
        name = output.get("name", "unknown")
        namespace = output.get("namespace")
        if not namespace:
            parts = name.split(".")
            if len(parts) == 3:
                # Interpret as DB.schema.table
                namespace = f"mssql://localhost/{parts[0]}"
                name = ".".join(parts[1:])
            else:
                namespace = "mssql://localhost/InfoTrackerDW"
        
        facets = output.get("facets", {})
        
        # Build schema from schema facet
        schema_facet = facets.get("schema", {})
        columns = []
        if "fields" in schema_facet:
            for i, field in enumerate(schema_facet["fields"]):
                columns.append(ColumnSchema(
                    name=field.get("name", "unknown"),
                    data_type=field.get("type", "unknown"),
                    nullable=True,  # Default assumption
                    ordinal=i
                ))
        
        schema = TableSchema(
            namespace=namespace,
            name=name,
            columns=columns
        )
        
        # Build lineage from columnLineage facet
        lineage = []
        lineage_facet = facets.get("columnLineage", {})
        if "fields" in lineage_facet:
            for output_col, lineage_info in lineage_facet["fields"].items():
                input_fields = []
                for input_field in lineage_info.get("inputFields", []):
                    input_fields.append(ColumnReference(
                        namespace=input_field.get("namespace", namespace),
                        table_name=input_field.get("name", "unknown"),
                        column_name=input_field.get("field", "unknown")
                    ))
                
                transformation_type_str = lineage_info.get("transformationType", "IDENTITY")
                try:
                    transformation_type = TransformationType(transformation_type_str)
                except ValueError:
                    transformation_type = TransformationType.IDENTITY
                
                lineage.append(ColumnLineage(
                    output_column=output_col,
                    input_fields=input_fields,
                    transformation_type=transformation_type,
                    transformation_description=lineage_info.get("transformationDescription", "")
                ))
        
        # Build dependencies from inputs
        dependencies = set()
        for input_obj in artifact.get("inputs", []):
            input_name = input_obj.get("name", "")
            if input_name:
                dependencies.add(input_name)
        
        # Determine object type
        object_type = "view" if lineage else "table"
        
        return ObjectInfo(
            name=name,
            object_type=object_type,
            schema=schema,
            lineage=lineage,
            dependencies=dependencies
        )


def qualify_identifier(identifier: str, default_database: Optional[str] = None) -> str:
    """Qualify a SQL identifier with default database when needed.
    
    Args:
        identifier: The identifier to qualify (can be 1, 2, or 3 parts)
        default_database: Default database to use when not specified
        
    Returns:
        Fully qualified identifier
    """
    if not identifier:
        return identifier
        
    parts = identifier.split('.')
    
    if len(parts) == 1:
        # Just table name - add schema and database
        if default_database:
            return f"{default_database}.dbo.{parts[0]}"
        else:
            return f"dbo.{parts[0]}"
    elif len(parts) == 2:
        # schema.table - add database
        if default_database:
            return f"{default_database}.{identifier}"
        else:
            return identifier
    else:
        # Already fully qualified
        return identifier


def sanitize_name(name: str) -> str:
    """Sanitize object name by removing trailing semicolons and whitespace.
    
    Args:
        name: Object name to sanitize
        
    Returns:
        Sanitized name
    """
    if not name:
        return name
    
    # Remove trailing semicolons and whitespace
    return name.rstrip(';').strip()

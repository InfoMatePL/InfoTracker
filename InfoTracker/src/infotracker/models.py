"""
Core data models for InfoTracker.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from enum import Enum


class TransformationType(Enum):
    """Types of column transformations."""
    IDENTITY = "IDENTITY"
    CAST = "CAST"
    CASE = "CASE"
    AGGREGATE = "AGGREGATE"
    EXPRESSION = "EXPRESSION"
    CONCAT = "CONCAT"
    ARITHMETIC = "ARITHMETIC"


@dataclass
class ColumnReference:
    """Reference to a specific column in a table/view."""
    namespace: str
    table_name: str
    column_name: str
    
    def __str__(self) -> str:
        return f"{self.namespace}.{self.table_name}.{self.column_name}"


@dataclass
class ColumnSchema:
    """Schema information for a column."""
    name: str
    data_type: str
    nullable: bool = True
    ordinal: int = 0


@dataclass
class TableSchema:
    """Schema information for a table/view."""
    namespace: str
    name: str
    columns: List[ColumnSchema] = field(default_factory=list)
    
    def get_column(self, name: str) -> Optional[ColumnSchema]:
        """Get column by name (case-insensitive for SQL Server)."""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None


@dataclass
class ColumnLineage:
    """Lineage information for a single output column."""
    output_column: str
    input_fields: List[ColumnReference] = field(default_factory=list)
    transformation_type: TransformationType = TransformationType.IDENTITY
    transformation_description: str = ""


@dataclass
class ObjectInfo:
    """Information about a SQL object (table, view, etc.)."""
    name: str
    object_type: str  # "table", "view", "procedure"
    schema: TableSchema
    lineage: List[ColumnLineage] = field(default_factory=list)
    dependencies: Set[str] = field(default_factory=set)  # Tables this object depends on


class SchemaRegistry:
    """Registry to store and resolve table schemas."""
    
    def __init__(self):
        self._schemas: Dict[str, TableSchema] = {}
    
    def register(self, schema: TableSchema) -> None:
        """Register a table schema."""
        key = f"{schema.namespace}.{schema.name}".lower()
        self._schemas[key] = schema
    
    def get(self, namespace: str, name: str) -> Optional[TableSchema]:
        """Get schema by namespace and name."""
        key = f"{namespace}.{name}".lower()
        return self._schemas.get(key)
    
    def get_all(self) -> List[TableSchema]:
        """Get all registered schemas."""
        return list(self._schemas.values())


class ObjectGraph:
    """Graph of SQL object dependencies."""
    
    def __init__(self):
        self._objects: Dict[str, ObjectInfo] = {}
        self._dependencies: Dict[str, Set[str]] = {}
    
    def add_object(self, obj: ObjectInfo) -> None:
        """Add an object to the graph."""
        key = obj.name.lower()
        self._objects[key] = obj
        self._dependencies[key] = obj.dependencies
    
    def get_object(self, name: str) -> Optional[ObjectInfo]:
        """Get object by name."""
        return self._objects.get(name.lower())
    
    def get_dependencies(self, name: str) -> Set[str]:
        """Get dependencies for an object."""
        return self._dependencies.get(name.lower(), set())
    
    def topological_sort(self) -> List[str]:
        """Return objects in topological order (dependencies first)."""
        # Simple topological sort implementation
        visited = set()
        temp_visited = set()
        result = []
        
        def visit(node: str):
            if node in temp_visited:
                # Cycle detected, but we'll handle gracefully
                return
            if node in visited:
                return
                
            temp_visited.add(node)
            for dep in self._dependencies.get(node, set()):
                if dep.lower() in self._dependencies:  # Only visit if we have the dependency
                    visit(dep.lower())
            
            temp_visited.remove(node)
            visited.add(node)
            result.append(node)
        
        for obj_name in self._objects:
            if obj_name not in visited:
                visit(obj_name)
        
        return result

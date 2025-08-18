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
    AGGREGATION = "AGGREGATION"
    ARITHMETIC_AGGREGATION = "ARITHMETIC_AGGREGATION"
    COMPLEX_AGGREGATION = "COMPLEX_AGGREGATION"
    EXPRESSION = "EXPRESSION"
    CONCAT = "CONCAT"
    ARITHMETIC = "ARITHMETIC"
    RENAME = "RENAME"
    UNION = "UNION"
    STRING_PARSE = "STRING_PARSE"
    WINDOW_FUNCTION = "WINDOW_FUNCTION"
    WINDOW = "WINDOW"
    DATE_FUNCTION = "DATE_FUNCTION"
    DATE_FUNCTION_AGGREGATION = "DATE_FUNCTION_AGGREGATION"
    CASE_AGGREGATION = "CASE_AGGREGATION"
    EXEC = "EXEC"


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


@dataclass
class ColumnNode:
    """Node in the column graph representing a fully qualified column."""
    namespace: str
    table_name: str
    column_name: str
    
    def __str__(self) -> str:
        return f"{self.namespace}.{self.table_name}.{self.column_name}"
    
    def __hash__(self) -> int:
        return hash((self.namespace.lower(), self.table_name.lower(), self.column_name.lower()))
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, ColumnNode):
            return False
        return (self.namespace.lower() == other.namespace.lower() and 
                self.table_name.lower() == other.table_name.lower() and
                self.column_name.lower() == other.column_name.lower())


@dataclass
class ColumnEdge:
    """Edge in the column graph representing lineage relationship."""
    from_column: ColumnNode
    to_column: ColumnNode
    transformation_type: TransformationType
    transformation_description: str


class ColumnGraph:
    """Bidirectional graph of column-level lineage relationships."""
    
    def __init__(self, max_upstream_depth: int = 10, max_downstream_depth: int = 10):
        """Initialize the column graph with configurable depth limits.
        
        Args:
            max_upstream_depth: Maximum depth for upstream traversal (default: 10)
            max_downstream_depth: Maximum depth for downstream traversal (default: 10)
        """
        self._nodes: Dict[str, ColumnNode] = {}
        self._upstream_edges: Dict[str, List[ColumnEdge]] = {}  # node -> edges coming into it
        self._downstream_edges: Dict[str, List[ColumnEdge]] = {}  # node -> edges going out of it
        self.max_upstream_depth = max_upstream_depth
        self.max_downstream_depth = max_downstream_depth
    
    def add_node(self, column_node: ColumnNode) -> None:
        """Add a column node to the graph."""
        key = str(column_node).lower()
        self._nodes[key] = column_node
        if key not in self._upstream_edges:
            self._upstream_edges[key] = []
        if key not in self._downstream_edges:
            self._downstream_edges[key] = []
    
    def add_edge(self, edge: ColumnEdge) -> None:
        """Add a lineage edge to the graph."""
        from_key = str(edge.from_column).lower()
        to_key = str(edge.to_column).lower()
        
        # Ensure nodes exist
        self.add_node(edge.from_column)
        self.add_node(edge.to_column)
        
        # Add edge to both directions
        self._downstream_edges[from_key].append(edge)
        self._upstream_edges[to_key].append(edge)
    
    def get_upstream(self, column: ColumnNode, max_depth: Optional[int] = None) -> List[ColumnEdge]:
        """Get all upstream dependencies for a column.
        
        Args:
            column: The column to find upstream dependencies for
            max_depth: Override the default max_upstream_depth for this query
        """
        effective_depth = max_depth if max_depth is not None else self.max_upstream_depth
        return self._traverse_upstream(column, effective_depth, set())
    
    def get_downstream(self, column: ColumnNode, max_depth: Optional[int] = None) -> List[ColumnEdge]:
        """Get all downstream dependencies for a column.
        
        Args:
            column: The column to find downstream dependencies for
            max_depth: Override the default max_downstream_depth for this query
        """
        effective_depth = max_depth if max_depth is not None else self.max_downstream_depth
        return self._traverse_downstream(column, effective_depth, set())
    
    def _traverse_upstream(self, column: ColumnNode, max_depth: int, visited: Set[str], current_depth: int = 0) -> List[ColumnEdge]:
        """Recursively traverse upstream dependencies."""
        if max_depth <= 0 or current_depth >= max_depth:
            return []
        
        column_key = str(column).lower()
        if column_key in visited:
            return []  # Avoid cycles
        
        visited.add(column_key)
        edges = []
        
        # Get direct upstream edges
        for edge in self._upstream_edges.get(column_key, []):
            edges.append(edge)
            # Recursively get upstream of the source column
            upstream_edges = self._traverse_upstream(edge.from_column, max_depth, visited.copy(), current_depth + 1)
            edges.extend(upstream_edges)
        
        return edges
    
    def _traverse_downstream(self, column: ColumnNode, max_depth: int, visited: Set[str], current_depth: int = 0) -> List[ColumnEdge]:
        """Recursively traverse downstream dependencies."""
        if max_depth <= 0 or current_depth >= max_depth:
            return []
        
        column_key = str(column).lower()
        if column_key in visited:
            return []  # Avoid cycles
        
        visited.add(column_key)
        edges = []
        
        # Get direct downstream edges
        for edge in self._downstream_edges.get(column_key, []):
            edges.append(edge)
            # Recursively get downstream of the target column
            downstream_edges = self._traverse_downstream(edge.to_column, max_depth, visited.copy(), current_depth + 1)
            edges.extend(downstream_edges)
        
        return edges
    
    def get_traversal_stats(self, column: ColumnNode) -> Dict[str, Any]:
        """Get traversal statistics for a column including depth information.
        
        Returns:
            Dictionary with upstream/downstream counts and depth information
        """
        upstream_edges = self.get_upstream(column)
        downstream_edges = self.get_downstream(column)
        
        return {
            "column": str(column),
            "upstream_count": len(upstream_edges),
            "downstream_count": len(downstream_edges),
            "max_upstream_depth": self.max_upstream_depth,
            "max_downstream_depth": self.max_downstream_depth,
            "upstream_tables": len(set(str(edge.from_column).rsplit('.', 1)[0] for edge in upstream_edges)),
            "downstream_tables": len(set(str(edge.to_column).rsplit('.', 1)[0] for edge in downstream_edges))
        }
    
    def build_from_object_lineage(self, objects: List[ObjectInfo]) -> None:
        """Build column graph from object lineage information."""
        for obj in objects:
            output_namespace = obj.schema.namespace
            output_table = obj.schema.name
            
            for lineage in obj.lineage:
                # Create output column node
                output_column = ColumnNode(
                    namespace=output_namespace,
                    table_name=output_table,
                    column_name=lineage.output_column
                )
                
                # Create edges for each input field
                for input_field in lineage.input_fields:
                    input_column = ColumnNode(
                        namespace=input_field.namespace,
                        table_name=input_field.table_name,
                        column_name=input_field.column_name
                    )
                    
                    edge = ColumnEdge(
                        from_column=input_column,
                        to_column=output_column,
                        transformation_type=lineage.transformation_type,
                        transformation_description=lineage.transformation_description
                    )
                    
                    self.add_edge(edge)
    
    def find_column(self, selector: str) -> Optional[ColumnNode]:
        """Find a column by selector string (namespace.table.column)."""
        selector_key = selector.lower()
        return self._nodes.get(selector_key)
    
    
    def find_columns_wildcard(self, selector: str) -> List[ColumnNode]:
            """
            Find columns matching a wildcard pattern.

            Supports:
            - Table wildcard:   <ns>.<schema>.<table>.*     → all columns of that table
            - Column wildcard:  <optional_ns>..<pattern>    → match by COLUMN NAME only:
                * if pattern contains any of [*?[]] → fnmatch on the column name
                * otherwise → default to case-insensitive "contains"
            - Fallback:         fnmatch on the full identifier "ns.schema.table.column"
            """
            import fnmatch as _fn

            sel = (selector or "").strip().lower()

            # 1) Table wildcard: "...schema.table.*"
            if sel.endswith(".*"):
                table_sel = sel[:-1]  # remove trailing '*', keep final dot
                # simple prefix match on full key
                return [node for key, node in self._nodes.items() if key.startswith(table_sel)]

            # 2) Column wildcard: "<optional_ns>..<pattern>"
            if ".." in sel:
                ns_part, col_pat = sel.split("..", 1)
                ns_part = ns_part.strip(".")
                col_pat = col_pat.strip()

                # if no explicit wildcard meta, treat as "contains"
                has_meta = any(ch in col_pat for ch in "*?[]")

                def col_name_matches(name: str) -> bool:
                    name = (name or "").lower()
                    if has_meta:
                        return _fn.fnmatch(name, col_pat)
                    return col_pat in name  # default: contains (case-insensitive)

                if ns_part:
                    ns_prefix = ns_part + "."
                    return [
                        node
                        for key, node in self._nodes.items()
                        if key.startswith(ns_prefix) and col_name_matches(getattr(node, "column_name", ""))
                    ]
                else:
                    return [node for node in self._nodes.values() if col_name_matches(getattr(node, "column_name", ""))]

            # 3) Fallback: fnmatch on the full identifier
            return [node for key, node in self._nodes.items() if _fn.fnmatch(key, sel)]

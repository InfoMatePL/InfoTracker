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
    CONSTANT = "CONSTANT"
    UNKNOWN = "UNKNOWN"


@dataclass
class ColumnReference:
    """Reference to a specific column in a table/view."""
    namespace: str
    table_name: str
    column_name: str
    
    def __post_init__(self) -> None:
        # Ensure table_name is always schema.table (strip database if present).
        # Leave temp tables as-is (e.g., "tempdb..#tmp").
        if self.table_name:
            # Skip tempdb pattern
            if self.table_name.startswith('tempdb..#') or self.table_name.startswith('#'):
                return
            parts = self.table_name.split('.')
            if len(parts) >= 3:
                self.table_name = '.'.join(parts[-2:])
    
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
    is_fallback: bool = field(default=False)  # Whether this was created by fallback parsing
    no_output_reason: Optional[str] = field(default=None)  # Reason for no persistent output


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

            # Normalize: strip leading '<DB>.' from table name if it matches namespace DB
            try:
                ns_db = output_namespace.rsplit('/', 1)[1]
            except Exception:
                ns_db = None
            if ns_db and output_table and output_table.startswith(f"{ns_db}."):
                output_table = output_table[len(ns_db) + 1:]
            
            for lineage in obj.lineage:
                # Create output column node
                output_column = ColumnNode(
                    namespace=output_namespace,
                    table_name=output_table,
                    column_name=lineage.output_column
                )
                
                # Create edges for each input field
                for input_field in lineage.input_fields:
                    in_ns = input_field.namespace
                    in_tbl = input_field.table_name
                    # Normalize inputs similarly
                    try:
                        in_db = in_ns.rsplit('/', 1)[1] if in_ns else None
                    except Exception:
                        in_db = None
                    if in_db and in_tbl and in_tbl.startswith(f"{in_db}."):
                        in_tbl = in_tbl[len(in_db) + 1:]

                    input_column = ColumnNode(
                        namespace=in_ns,
                        table_name=in_tbl,
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
        - Column wildcard:  <optional_ns>..<pattern>    → match by COLUMN NAME only
        - Fallback:         fnmatch on the full identifier "ns.schema.table.column"
        """
        import fnmatch as _fn
        
        # 1) Normalizacja i szybkie wyjścia
        sel = (selector or "").strip()
        low = sel.lower()
        
        # Pusty/niepełny wzorzec
        if low in {".", ".."}:
            return []
        
        if ".." in low:
            ns_part, col_pat = low.split("..", 1)
            if col_pat.strip() == "":
                return []
        
        # 2) Table wildcard "….*" – obsłuż W OBU wariantach (z i bez namespace)
        if low.endswith(".*"):
            left = sel[:-2].strip()
            if not left:
                return []
            
            # Lokalny helper do dopasowania tabel
            def _tbl_match(left: str, node_tbl: str) -> bool:
                lp = (left or "").lower().split(".")
                tp = (node_tbl or "").lower().split(".")
                # dopasuj po końcówce: 3, 2 albo 1 segment
                if len(lp) >= 3:
                    return tp[-3:] == lp[-3:] or tp[-2:] == lp[-2:]
                elif len(lp) == 2:
                    return tp[-2:] == lp[-2:]
                else:
                    return tp[-1] == lp[-1] if lp else False
            
            if "://" in left:
                # Z namespace - bardziej dokładne parsowanie
                # Format: mssql://localhost/InfoTrackerDW.STG.dbo.Orders
                if "." in left:
                    # Znajdź pierwszą kropkę po namespace
                    ns_end = left.find(".") 
                    ns = left[:ns_end]
                    table = left[ns_end + 1:]
                    
                    results = [
                        node for node in self._nodes.values()
                        if (node.namespace and node.namespace.lower().startswith(ns.lower()) and
                            _tbl_match(table, node.table_name))
                    ]
                else:
                    results = []
            else:
                # Bez namespace
                results = [
                    node for node in self._nodes.values()
                    if _tbl_match(left, node.table_name)
                ]
            
            # Deduplikacja
            tmp = {}
            for n in results:
                tmp[str(n).lower()] = n
            return list(tmp.values())
        
        # 3) Column wildcard "<opcjonalny_prefix>..<column_pattern>" – dodaj semantykę CONTAINS
        if ".." in low:
            ns_part, col_pat = low.split("..", 1)
            col_pat = col_pat.strip()
            if col_pat == "":
                return []
            
            # Sprawdź czy są wildcardy
            has_wildcards = any(ch in col_pat for ch in "*?[]")
            
            def col_match(name: str) -> bool:
                n = (name or "").lower()
                return _fn.fnmatch(n, col_pat) if has_wildcards else (col_pat in n)
            
            if ns_part:
                ns_part = ns_part.strip(".")
                if "://" in ns_part:
                    # Sprawdź czy po namespace jest kropka - wtedy reszta to prefiks tabeli
                    if "." in ns_part:
                        # Znajdź część po pierwszej kropce po namespace jako prefiks tabeli
                        first_dot = ns_part.find(".")
                        table_prefix = ns_part[first_dot + 1:].lower()
                        results = [
                            node for node in self._nodes.values()
                            if (node.table_name and node.table_name.lower().startswith(table_prefix) and
                                col_match(node.column_name))
                        ]
                    else:
                        # Tylko namespace, bez prefiksu tabeli
                        results = [
                            node for node in self._nodes.values()
                            if (node.namespace and node.namespace.lower().startswith(ns_part) and
                                col_match(node.column_name))
                        ]
                else:
                    # Brak namespace - traktuj jako prefiks tabeli
                    results = [
                        node for node in self._nodes.values()
                        if (node.table_name and node.table_name.lower().startswith(ns_part) and
                            col_match(node.column_name))
                    ]
            else:
                results = [
                    node for node in self._nodes.values() 
                    if col_match(node.column_name)
                ]
            
            # Deduplikacja
            tmp = {}
            for n in results:
                tmp[str(n).lower()] = n
            return list(tmp.values())
        
        # 4) Fallback na pełnym kluczu
        if not any(ch in selector for ch in "*?[]"):
            # Potraktuj jako "contains" po pełnym kluczu
            results = [
                node for key, node in self._nodes.items() 
                if low in key.lower()
            ]
        else:
            # Są wildcardy - użyj fnmatch
            results = [
                node for key, node in self._nodes.items() 
                if _fn.fnmatch(key.lower(), low)
            ]
        
        # Deduplikacja
        tmp = {}
        for n in results:
            tmp[str(n).lower()] = n
        return list(tmp.values())

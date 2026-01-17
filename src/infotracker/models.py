"""
Core data models for InfoTracker.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
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
    
    def __hash__(self) -> int:
        """Case-insensitive hash for SQL Server compatibility."""
        return hash((self.namespace.lower(), self.table_name.lower(), self.column_name.lower()))
    
    def __eq__(self, other) -> bool:
        """Case-insensitive equality for SQL Server compatibility."""
        if not isinstance(other, ColumnReference):
            return False
        return (self.namespace.lower() == other.namespace.lower() and 
                self.table_name.lower() == other.table_name.lower() and
                self.column_name.lower() == other.column_name.lower())


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
    
    def __post_init__(self) -> None:
        """Preserve column_name casing; comparisons are case-insensitive elsewhere."""
        # Keep original casing for display/output, while hash/eq and graph keys
        # still use lowercase for SQL Server case-insensitive matching.
    
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
        # If max_depth == 0 → unlimited traversal. Only stop when max_depth > 0 and reached.
        if max_depth > 0 and current_depth >= max_depth:
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
        # If max_depth == 0 → unlimited traversal. Only stop when max_depth > 0 and reached.
        if max_depth > 0 and current_depth >= max_depth:
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
    
    def build_from_object_lineage(self, objects: List[ObjectInfo], cte_data: Dict[str, Any] = None) -> None:
        """Build column graph from object lineage information.
        
        Args:
            objects: List of ObjectInfo with lineage
            cte_data: Optional CTE registry for expanding CTE to base sources (NOT WORKING due to architectural limitation)
        """
        if cte_data is None:
            cte_data = {}
        
        # NOTE: CTE expansion was attempted but cte_data is always empty
        # CTE are registered locally in SelectLineageExtractor and don't propagate to engine/models
        # Known limitation: CTE will appear in column_graph as intermediate nodes
        
        # Build a map of old temp table names to new ones (e.g., "dbo.#asefl_temp" -> "dbo.update_asefl_TrialBalance_BV#asefl_temp")
        # Also build a map of temp table names to their ObjectInfo for lineage expansion
        # IMPORTANT: Owner-aware mapping to prevent cross-procedure temp table edges
        temp_name_map: Dict[str, str] = {}
        temp_obj_map: Dict[str, ObjectInfo] = {}  # temp table name -> ObjectInfo
        temp_owners_registry: Dict[str, Set[str]] = {}  # temp_raw (e.g., "#temp") -> set of owner procedures
        
        for obj in objects:
            if obj.object_type == "temp_table" and obj.schema.name:
                # obj.schema.name is in new format: "dbo.update_asefl_TrialBalance_BV#asefl_temp" (or with DB prefix)
                # Normalize: strip leading '<DB>.' from table name if it matches namespace DB
                normalized_name = obj.schema.name
                try:
                    ns_db = obj.schema.namespace.rsplit('/', 1)[1] if obj.schema.namespace else None
                    if ns_db and normalized_name.startswith(f"{ns_db}."):
                        normalized_name = normalized_name[len(ns_db) + 1:]
                except Exception:
                    pass
                # Extract temp table name (part after last '#') and owner (part before '#')
                if '#' in normalized_name:
                    temp_part = normalized_name.split('#')[-1]
                    owner_part = normalized_name.rsplit('#', 1)[0]  # Everything before last '#'
                    
                    # Register in owners_registry for uniqueness checks
                    temp_raw = f"#{temp_part}"
                    if temp_raw not in temp_owners_registry:
                        temp_owners_registry[temp_raw] = set()
                    temp_owners_registry[temp_raw].add(owner_part)
                    
                    # Map owner-aware keys (exact match, highest priority)
                    # Format: "dbo.update_X#temp" -> "dbo.update_X#temp"
                    temp_name_map[normalized_name] = normalized_name
                    if ns_db:
                        temp_name_map[f"{ns_db}.{normalized_name}"] = normalized_name
                    
                    # Map fallback keys (lower priority, used only if unique owner)
                    # These will be used only if temp_owners_registry shows single owner
                    old_name = f"dbo.#{temp_part}"
                    temp_name_map[old_name] = normalized_name
                    temp_name_map[temp_raw] = normalized_name
                    if ns_db:
                        temp_name_map[f"{ns_db}.dbo.#{temp_part}"] = normalized_name
                        temp_name_map[f"{ns_db}.#{temp_part}"] = normalized_name
                    
                    # Map in temp_obj_map for lineage expansion
                    temp_obj_map[normalized_name] = obj
                    if ns_db:
                        temp_obj_map[f"{ns_db}.{normalized_name}"] = obj
        
        for obj in objects:
            output_namespace = obj.schema.namespace
            output_table = obj.schema.name

            # Skip table variables (starting with @) - they should not appear in column_graph
            if output_table and output_table.startswith('@'):
                continue

            # Normalize: strip leading '<DB>.' from table name if it matches namespace DB
            try:
                ns_db = output_namespace.rsplit('/', 1)[1]
            except Exception:
                ns_db = None
            if ns_db and output_table and output_table.startswith(f"{ns_db}."):
                output_table = output_table[len(ns_db) + 1:]
            
            # Skip objects with table_name="unknown" (e.g., unresolved objects)
            if output_table == "unknown":
                continue
            
            # Add nodes for all columns, even if they don't have edges
            for col in obj.schema.columns or []:
                output_column = ColumnNode(
                    namespace=output_namespace,
                    table_name=output_table,
                    column_name=col.name
                )
                self.add_node(output_column)
            
            for lineage in obj.lineage:
                # Create output column node
                output_column = ColumnNode(
                    namespace=output_namespace,
                    table_name=output_table,
                    column_name=lineage.output_column
                )
                
                # Create edges for each input field
                for input_field in lineage.input_fields:
                    # Skip table variables (starting with @) - they should not appear in column_graph
                    if input_field.table_name and input_field.table_name.startswith('@'):
                        continue
                    # Skip "unknown" table names (e.g., unresolved JOIN keywords)
                    if input_field.table_name == "unknown":
                        continue
                    
                    in_ns = input_field.namespace
                    in_tbl = input_field.table_name
                    # Normalize inputs similarly
                    try:
                        in_db = in_ns.rsplit('/', 1)[1] if in_ns else None
                    except Exception:
                        in_db = None
                    
                    # Check temp_name_map BEFORE normalizing DB prefix, as map may contain DB-prefixed keys
                    # OWNER-AWARE LOOKUP: Prefer exact match, then fallback only if unique owner
                    original_tbl = in_tbl
                    temp_obj = None
                    if in_tbl and '#' in in_tbl:
                        # Extract temp_raw for uniqueness check
                        temp_part = in_tbl.split('#')[-1] if '#' in in_tbl else None
                        temp_raw = f"#{temp_part}" if temp_part else None
                        
                        # Build variants in priority order:
                        # 1. Exact match (owner-aware): "dbo.update_X#temp"
                        # 2. With DB prefix: "EDW_CORE.dbo.update_X#temp"
                        # 3. Fallback (only if unique owner): "#temp", "dbo.#temp"
                        exact_variants = [in_tbl]
                        if in_db and not in_tbl.startswith(f"{in_db}."):
                            exact_variants.append(f"{in_db}.{in_tbl}")
                        if in_db and in_tbl.startswith(f"{in_db}."):
                            exact_variants.append(in_tbl[len(in_db) + 1:])
                        
                        fallback_variants = []
                        if temp_raw:
                            # Only use fallback if this temp has UNIQUE owner
                            is_unique = temp_raw in temp_owners_registry and len(temp_owners_registry[temp_raw]) == 1
                            if is_unique:
                                fallback_variants.append(temp_raw)
                                fallback_variants.append(f"dbo.{temp_raw}")
                                if in_db:
                                    fallback_variants.append(f"{in_db}.dbo.{temp_raw}")
                                    fallback_variants.append(f"{in_db}.{temp_raw}")
                        
                        # Try exact variants first
                        mapped_tbl = None
                        for variant in exact_variants:
                            if variant in temp_name_map:
                                mapped_tbl = temp_name_map[variant]
                                break
                        
                        # If no exact match, try fallback (only if unique owner)
                        if not mapped_tbl:
                            for variant in fallback_variants:
                                if variant in temp_name_map:
                                    mapped_tbl = temp_name_map[variant]
                                    break
                        
                        # If we found a mapping, also check if we have temp_obj for lineage expansion
                        if mapped_tbl:
                            in_tbl = mapped_tbl
                            # Try to find temp_obj for this temp table - try multiple variants
                            search_variants = [mapped_tbl]
                            if in_db:
                                search_variants.append(f"{in_db}.{mapped_tbl}")
                            # Try all variants
                            for variant in search_variants:
                                if variant in temp_obj_map:
                                    temp_obj = temp_obj_map[variant]
                                    break
                            # If still not found, try to find by matching the temp part
                            if not temp_obj and '#' in mapped_tbl:
                                temp_part = mapped_tbl.split('#')[-1]
                                for key, obj in temp_obj_map.items():
                                    if key.endswith(f"#{temp_part}") or key == f"#{temp_part}":
                                        temp_obj = obj
                                        break
                    
                    # CHECK FOR CTE EXPANSION (before temp table expansion)
                    # If input is a CTE, expand it to base sources (similar to temp tables)
                    # CTE are ephemeral query-scoped objects that should be transparent in lineage
                    cte_expanded = False
                    if cte_data and in_tbl:
                        # Check if in_tbl is a CTE (case-insensitive match)
                        # Try simple name (last part after dots)
                        cte_simple = in_tbl.split('.')[-1] if '.' in in_tbl else in_tbl
                        cte_info = None
                        for cte_name, info in cte_data.items():
                            if cte_name.lower() == cte_simple.lower():
                                cte_info = info
                                break
                        
                        if cte_info:
                            # CTE found - extract base sources from CTE definition
                            # cte_info is either dict with 'definition' or just columns list
                            cte_def = None
                            if isinstance(cte_info, dict) and 'definition' in cte_info:
                                cte_def = cte_info['definition']
                                from sqlglot import expressions as exp
                                if cte_def and isinstance(cte_def, exp.Select):
                                    # Extract dependencies from CTE definition (base tables)
                                    # We can't call parser methods here, so use simple FROM extraction
                                    base_tables = set()
                                    for table in cte_def.find_all(exp.Table):
                                        tbl_name = str(table.name) if hasattr(table, 'name') else str(table)
                                        # Skip if this is another CTE
                                        is_another_cte = False
                                        for other_cte_name in cte_data.keys():
                                            if tbl_name.lower() == other_cte_name.lower():
                                                is_another_cte = True
                                                break
                                        if not is_another_cte and not tbl_name.startswith('#'):
                                            base_tables.add(tbl_name)
                                    
                                    # Create edges from CTE base sources to output (expand CTE)
                                    if base_tables:
                                        for base_tbl_name in base_tables:
                                            # Skip "unknown" table names
                                            if base_tbl_name == "unknown":
                                                continue
                                            # Use same namespace as CTE
                                            base_column = ColumnNode(
                                                namespace=in_ns,
                                                table_name=f"dbo.{base_tbl_name}" if '.' not in base_tbl_name else base_tbl_name,
                                                column_name=input_field.column_name
                                            )
                                            
                                            edge = ColumnEdge(
                                                from_column=base_column,
                                                to_column=output_column,
                                                transformation_type=lineage.transformation_type,
                                                transformation_description=lineage.transformation_description
                                            )
                                            
                                            self.add_edge(edge)
                                        cte_expanded = True
                                        # Skip creating edge from CTE itself - it's been expanded
                                        continue
                    
                    # If input is a temp table and we have its ObjectInfo, expand to base sources
                    if temp_obj and temp_obj.lineage:
                        # Find lineage for this column in temp table
                        temp_lineage = next((ln for ln in temp_obj.lineage if ln.output_column == input_field.column_name), None)
                        if temp_lineage and temp_lineage.input_fields:
                            # Filter out self-references (temp table referencing itself)
                            # Only include base sources (not temp tables)
                            base_inputs = []
                            for base_input in temp_lineage.input_fields:
                                base_tbl_name = base_input.table_name or ""
                                # Skip if this is a self-reference to the temp table
                                if '#' in base_tbl_name:
                                    # Check if it's the same temp table (use in_tbl which is already mapped)
                                    temp_part = base_tbl_name.split('#')[-1] if '#' in base_tbl_name else ""
                                    if temp_part and in_tbl and temp_part in in_tbl:
                                        # This is a self-reference, skip it
                                        continue
                                # This is a base source, include it
                                base_inputs.append(base_input)
                            
                            # Expand temp table to base sources - create edges from base sources to output
                            if base_inputs:
                                for base_input in base_inputs:
                                    base_ns = base_input.namespace
                                    base_tbl = base_input.table_name
                                    # Skip "unknown" table names
                                    if base_tbl == "unknown":
                                        continue
                                    # Normalize base table name
                                    try:
                                        base_db = base_ns.rsplit('/', 1)[1] if base_ns else None
                                    except Exception:
                                        base_db = None
                                    if base_db and base_tbl and base_tbl.startswith(f"{base_db}."):
                                        base_tbl = base_tbl[len(base_db) + 1:]
                                    
                                    base_column = ColumnNode(
                                        namespace=base_ns,
                                        table_name=base_tbl,
                                        column_name=base_input.column_name
                                    )
                                    
                                    edge = ColumnEdge(
                                        from_column=base_column,
                                        to_column=output_column,
                                        transformation_type=lineage.transformation_type,
                                        transformation_description=lineage.transformation_description
                                    )
                                    
                                    self.add_edge(edge)
                                # Also keep the direct temp->output edge for continuity
                    
                    # Normalize DB prefix AFTER temp_name_map lookup
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

    def distances_downstream(self, start: ColumnNode, max_depth: int = 0) -> Dict[str, int]:
        """Compute BFS distances (levels) for downstream traversal from a start column.

        Returns mapping of column-key -> level (1 for direct children, etc.).
        If max_depth == 0, traverse without limit.
        """
        start_key = str(start).lower()
        dist: Dict[str, int] = {}
        q = deque([(start_key, 0)])
        seen = {start_key}
        while q:
            u, d = q.popleft()
            if max_depth > 0 and d >= max_depth:
                continue
            for e in self._downstream_edges.get(u, []) or []:
                v = str(e.to_column).lower()
                if v in seen:
                    continue
                level = d + 1
                dist[v] = level
                seen.add(v)
                q.append((v, level))
        return dist

    def distances_upstream(self, start: ColumnNode, max_depth: int = 0) -> Dict[str, int]:
        """Compute BFS distances (levels) for upstream traversal from a start column.

        Returns mapping of column-key -> level (1 for direct parents, etc.).
        If max_depth == 0, traverse without limit.
        """
        start_key = str(start).lower()
        dist: Dict[str, int] = {}
        q = deque([(start_key, 0)])
        seen = {start_key}
        while q:
            u, d = q.popleft()
            if max_depth > 0 and d >= max_depth:
                continue
            for e in self._upstream_edges.get(u, []) or []:
                v = str(e.from_column).lower()
                if v in seen:
                    continue
                level = d + 1
                dist[v] = level
                seen.add(v)
                q.append((v, level))
        return dist
    
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

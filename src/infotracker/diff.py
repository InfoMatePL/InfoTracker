"""
Breaking change detection for InfoTracker.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set, Any

from .models import ObjectInfo, ColumnSchema, ColumnLineage, TransformationType


class ChangeType(Enum):
    """Types of changes that can be detected."""
    COLUMN_ADDED = "COLUMN_ADDED"
    COLUMN_REMOVED = "COLUMN_REMOVED"
    COLUMN_RENAMED = "COLUMN_RENAMED"
    COLUMN_TYPE_CHANGED = "COLUMN_TYPE_CHANGED"
    COLUMN_NULLABILITY_CHANGED = "COLUMN_NULLABILITY_CHANGED"
    COLUMN_ORDER_CHANGED = "COLUMN_ORDER_CHANGED"
    LINEAGE_CHANGED = "LINEAGE_CHANGED"
    OBJECT_ADDED = "OBJECT_ADDED"
    OBJECT_REMOVED = "OBJECT_REMOVED"
    OBJECT_TYPE_CHANGED = "OBJECT_TYPE_CHANGED"


class Severity(Enum):
    """Severity levels for changes."""
    BREAKING = "BREAKING"
    POTENTIALLY_BREAKING = "POTENTIALLY_BREAKING"  
    NON_BREAKING = "NON_BREAKING"


@dataclass
class Change:
    """Represents a single change between two versions."""
    change_type: ChangeType
    severity: Severity
    object_name: str
    column_name: Optional[str] = None
    old_value: Any = None
    new_value: Any = None
    description: str = ""
    impact_count: int = 0  # Number of downstream columns affected


class BreakingChangeDetector:
    """Detects breaking changes between two sets of object information."""
    
    def __init__(self):
        self.changes: List[Change] = []
    
    def detect_changes(self, base_objects: List[ObjectInfo], head_objects: List[ObjectInfo]) -> List[Change]:
        """Detect changes between base and head object lists."""
        self.changes = []
        
        # Create lookup dictionaries
        base_map = {obj.name.lower(): obj for obj in base_objects}
        head_map = {obj.name.lower(): obj for obj in head_objects}
        
        # Find object-level changes
        self._detect_object_changes(base_map, head_map)
        
        # Find schema changes for existing objects
        common_objects = set(base_map.keys()) & set(head_map.keys())
        for obj_name in common_objects:
            self._detect_schema_changes(base_map[obj_name], head_map[obj_name])
            self._detect_lineage_changes(base_map[obj_name], head_map[obj_name])
        
        return self.changes
    
    def _detect_object_changes(self, base_map: Dict[str, ObjectInfo], head_map: Dict[str, ObjectInfo]) -> None:
        """Detect object additions, removals, and type changes."""
        base_names = set(base_map.keys())
        head_names = set(head_map.keys())
        
        # Object additions
        for added_name in head_names - base_names:
            obj = head_map[added_name]
            self.changes.append(Change(
                change_type=ChangeType.OBJECT_ADDED,
                severity=Severity.NON_BREAKING,
                object_name=obj.name,
                description=f"Added {obj.object_type} '{obj.name}'"
            ))
        
        # Object removals
        for removed_name in base_names - head_names:
            obj = base_map[removed_name]
            self.changes.append(Change(
                change_type=ChangeType.OBJECT_REMOVED,
                severity=Severity.BREAKING,
                object_name=obj.name,
                description=f"Removed {obj.object_type} '{obj.name}'"
            ))
        
        # Object type changes
        for common_name in base_names & head_names:
            base_obj = base_map[common_name]
            head_obj = head_map[common_name]
            
            if base_obj.object_type != head_obj.object_type:
                self.changes.append(Change(
                    change_type=ChangeType.OBJECT_TYPE_CHANGED,
                    severity=Severity.BREAKING,
                    object_name=base_obj.name,
                    old_value=base_obj.object_type,
                    new_value=head_obj.object_type,
                    description=f"Changed object type from {base_obj.object_type} to {head_obj.object_type}"
                ))
    
    def _detect_schema_changes(self, base_obj: ObjectInfo, head_obj: ObjectInfo) -> None:
        """Detect schema changes within an object."""
        base_columns = {col.name.lower(): col for col in base_obj.schema.columns}
        head_columns = {col.name.lower(): col for col in head_obj.schema.columns}
        
        base_names = set(base_columns.keys())
        head_names = set(head_columns.keys())
        
        # Column additions
        for added_name in head_names - base_names:
            col = head_columns[added_name]
            severity = Severity.POTENTIALLY_BREAKING  # Could affect SELECT *
            self.changes.append(Change(
                change_type=ChangeType.COLUMN_ADDED,
                severity=severity,
                object_name=base_obj.name,
                column_name=col.name,
                new_value=f"{col.data_type} {'NULL' if col.nullable else 'NOT NULL'}",
                description=f"Added column '{col.name}' ({col.data_type})"
            ))
        
        # Column removals
        for removed_name in base_names - head_names:
            col = base_columns[removed_name]
            self.changes.append(Change(
                change_type=ChangeType.COLUMN_REMOVED,
                severity=Severity.BREAKING,
                object_name=base_obj.name,
                column_name=col.name,
                old_value=f"{col.data_type} {'NULL' if col.nullable else 'NOT NULL'}",
                description=f"Removed column '{col.name}'"
            ))
        
        # Column changes for existing columns
        for common_name in base_names & head_names:
            base_col = base_columns[common_name]
            head_col = head_columns[common_name]
            
            # Type changes
            if base_col.data_type != head_col.data_type:
                severity = self._classify_type_change_severity(base_col.data_type, head_col.data_type)
                self.changes.append(Change(
                    change_type=ChangeType.COLUMN_TYPE_CHANGED,
                    severity=severity,
                    object_name=base_obj.name,
                    column_name=base_col.name,
                    old_value=base_col.data_type,
                    new_value=head_col.data_type,
                    description=f"Changed column '{base_col.name}' type from {base_col.data_type} to {head_col.data_type}"
                ))
            
            # Nullability changes
            if base_col.nullable != head_col.nullable:
                severity = Severity.BREAKING if not head_col.nullable else Severity.POTENTIALLY_BREAKING
                self.changes.append(Change(
                    change_type=ChangeType.COLUMN_NULLABILITY_CHANGED,
                    severity=severity,
                    object_name=base_obj.name,
                    column_name=base_col.name,
                    old_value="NULL" if base_col.nullable else "NOT NULL",
                    new_value="NULL" if head_col.nullable else "NOT NULL",
                    description=f"Changed column '{base_col.name}' nullability"
                ))
            
            # Ordinal changes (column order)
            if base_col.ordinal != head_col.ordinal:
                self.changes.append(Change(
                    change_type=ChangeType.COLUMN_ORDER_CHANGED,
                    severity=Severity.POTENTIALLY_BREAKING,
                    object_name=base_obj.name,
                    column_name=base_col.name,
                    old_value=base_col.ordinal,
                    new_value=head_col.ordinal,
                    description=f"Changed column '{base_col.name}' position from {base_col.ordinal} to {head_col.ordinal}"
                ))
    
    def _detect_lineage_changes(self, base_obj: ObjectInfo, head_obj: ObjectInfo) -> None:
        """Detect lineage changes for columns."""
        base_lineage = {lin.output_column.lower(): lin for lin in base_obj.lineage}
        head_lineage = {lin.output_column.lower(): lin for lin in head_obj.lineage}
        
        # Check for lineage changes in common columns
        for column_name in set(base_lineage.keys()) & set(head_lineage.keys()):
            base_lin = base_lineage[column_name]
            head_lin = head_lineage[column_name]
            
            # Compare transformation type
            if base_lin.transformation_type != head_lin.transformation_type:
                self.changes.append(Change(
                    change_type=ChangeType.LINEAGE_CHANGED,
                    severity=Severity.POTENTIALLY_BREAKING,
                    object_name=base_obj.name,
                    column_name=base_lin.output_column,
                    old_value=base_lin.transformation_type.value,
                    new_value=head_lin.transformation_type.value,
                    description=f"Changed transformation type for '{base_lin.output_column}'"
                ))
            
            # Compare input fields
            base_inputs = {(ref.table_name, ref.column_name) for ref in base_lin.input_fields}
            head_inputs = {(ref.table_name, ref.column_name) for ref in head_lin.input_fields}
            
            if base_inputs != head_inputs:
                self.changes.append(Change(
                    change_type=ChangeType.LINEAGE_CHANGED,
                    severity=Severity.POTENTIALLY_BREAKING,
                    object_name=base_obj.name,
                    column_name=base_lin.output_column,
                    old_value=len(base_inputs),
                    new_value=len(head_inputs),
                    description=f"Changed input dependencies for '{base_lin.output_column}'"
                ))
    
    def _classify_type_change_severity(self, old_type: str, new_type: str) -> Severity:
        """Classify the severity of a type change."""
        old_type = old_type.upper()
        new_type = new_type.upper()
        
        # Common safe widenings
        safe_widenings = [
            ("INT", "BIGINT"),
            ("DECIMAL(10,2)", "DECIMAL(18,2)"),
            ("VARCHAR(50)", "VARCHAR(100)"),
            ("NVARCHAR(50)", "NVARCHAR(100)"),
        ]
        
        if (old_type, new_type) in safe_widenings:
            return Severity.NON_BREAKING
        
        # Check for obvious narrowings
        if ("VARCHAR" in old_type and "VARCHAR" in new_type or
            "DECIMAL" in old_type and "DECIMAL" in new_type):
            return Severity.POTENTIALLY_BREAKING
        
        # Default to breaking for type changes
        return Severity.BREAKING
    
    def classify_by_severity(self) -> Dict[Severity, List[Change]]:
        """Group changes by severity level."""
        result = {severity: [] for severity in Severity}
        for change in self.changes:
            result[change.severity].append(change)
        return result
    
    def get_breaking_count(self) -> int:
        """Get count of breaking changes."""
        return len([c for c in self.changes if c.severity == Severity.BREAKING])
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of changes."""
        by_severity = self.classify_by_severity()
        return {
            "total_changes": len(self.changes),
            "breaking": len(by_severity[Severity.BREAKING]),
            "potentially_breaking": len(by_severity[Severity.POTENTIALLY_BREAKING]),
            "non_breaking": len(by_severity[Severity.NON_BREAKING]),
            "changes_by_type": self._count_by_type(),
            "changes": [self._change_to_dict(c) for c in self.changes]
        }
    
    def _count_by_type(self) -> Dict[str, int]:
        """Count changes by type."""
        counts = {}
        for change in self.changes:
            change_type = change.change_type.value
            counts[change_type] = counts.get(change_type, 0) + 1
        return counts
    
    def _change_to_dict(self, change: Change) -> Dict[str, Any]:
        """Convert change to dictionary for JSON serialization."""
        return {
            "change_type": change.change_type.value,
            "severity": change.severity.value,
            "object_name": change.object_name,
            "column_name": change.column_name,
            "old_value": change.old_value,
            "new_value": change.new_value,
            "description": change.description,
            "impact_count": change.impact_count
        }

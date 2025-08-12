"""
SQL parsing and lineage extraction using SQLGlot.
"""
from __future__ import annotations

import re
from typing import List, Optional, Set, Dict, Any

import sqlglot
from sqlglot import expressions as exp

from .models import (
    ColumnReference, ColumnSchema, TableSchema, ColumnLineage, 
    TransformationType, ObjectInfo, SchemaRegistry
)


class SqlParser:
    """Parser for SQL statements using SQLGlot."""
    
    def __init__(self, dialect: str = "tsql"):
        self.dialect = dialect
        self.schema_registry = SchemaRegistry()
    
    def parse_sql_file(self, sql_content: str, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse a SQL file and extract object information."""
        try:
            # Parse the SQL statement
            statements = sqlglot.parse(sql_content, read=self.dialect)
            if not statements:
                raise ValueError("No valid SQL statements found")
            
            # For now, handle single statement per file
            statement = statements[0]
            
            if isinstance(statement, exp.Create):
                return self._parse_create_statement(statement, object_hint)
            else:
                raise ValueError(f"Unsupported statement type: {type(statement)}")
                
        except Exception as e:
            # Return an object with error information
            return ObjectInfo(
                name=object_hint or "unknown",
                object_type="unknown",
                schema=TableSchema(
                    namespace="mssql://localhost/InfoTrackerDW",
                    name=object_hint or "unknown",
                    columns=[]
                ),
                lineage=[],
                dependencies=set()
            )
    
    def _parse_create_statement(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse CREATE TABLE or CREATE VIEW statement."""
        if statement.kind == "TABLE":
            return self._parse_create_table(statement, object_hint)
        elif statement.kind == "VIEW":
            return self._parse_create_view(statement, object_hint)
        else:
            raise ValueError(f"Unsupported CREATE statement: {statement.kind}")
    
    def _parse_create_table(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse CREATE TABLE statement."""
        # Extract table name and schema from statement.this (which is a Schema object)
        schema_expr = statement.this
        table_name = self._get_table_name(schema_expr.this, object_hint)
        namespace = "mssql://localhost/InfoTrackerDW"
        
        # Extract columns from the schema expressions
        columns = []
        if hasattr(schema_expr, 'expressions') and schema_expr.expressions:
            for i, column_def in enumerate(schema_expr.expressions):
                if isinstance(column_def, exp.ColumnDef):
                    col_name = str(column_def.this)
                    col_type = self._extract_column_type(column_def)
                    nullable = not self._has_not_null_constraint(column_def)
                    
                    columns.append(ColumnSchema(
                        name=col_name,
                        data_type=col_type,
                        nullable=nullable,
                        ordinal=i
                    ))
        
        schema = TableSchema(
            namespace=namespace,
            name=table_name,
            columns=columns
        )
        
        # Register schema for future reference
        self.schema_registry.register(schema)
        
        return ObjectInfo(
            name=table_name,
            object_type="table",
            schema=schema,
            lineage=[],  # Tables don't have lineage, they are sources
            dependencies=set()
        )
    
    def _parse_create_view(self, statement: exp.Create, object_hint: Optional[str] = None) -> ObjectInfo:
        """Parse CREATE VIEW statement."""
        view_name = self._get_table_name(statement.this, object_hint)
        namespace = "mssql://localhost/InfoTrackerDW"
        
        # Get the SELECT statement
        select_stmt = statement.expression
        if not isinstance(select_stmt, exp.Select):
            raise ValueError("VIEW must contain a SELECT statement")
        
        # Extract dependencies (tables referenced in FROM/JOIN)
        dependencies = self._extract_dependencies(select_stmt)
        
        # Extract column lineage
        lineage, output_columns = self._extract_column_lineage(select_stmt, view_name)
        
        schema = TableSchema(
            namespace=namespace,
            name=view_name,
            columns=output_columns
        )
        
        # Register schema for future reference
        self.schema_registry.register(schema)
        
        return ObjectInfo(
            name=view_name,
            object_type="view",
            schema=schema,
            lineage=lineage,
            dependencies=dependencies
        )
    
    def _get_table_name(self, table_expr: exp.Expression, hint: Optional[str] = None) -> str:
        """Extract table name from expression."""
        if isinstance(table_expr, exp.Table):
            # Handle qualified names like dbo.table_name
            if table_expr.db:
                return f"{table_expr.db}.{table_expr.name}"
            return str(table_expr.name)
        elif isinstance(table_expr, exp.Identifier):
            return str(table_expr.this)
        return hint or "unknown"
    
    def _extract_column_type(self, column_def: exp.ColumnDef) -> str:
        """Extract column type from column definition."""
        if column_def.kind:
            data_type = str(column_def.kind)
            # Convert to match expected format (lowercase for simple types)
            if data_type.upper().startswith('VARCHAR'):
                data_type = data_type.replace('VARCHAR', 'nvarchar')
            elif data_type.upper() == 'INT':
                data_type = 'int'
            elif data_type.upper() == 'DATE':
                data_type = 'date'
            return data_type.lower()
        return "unknown"
    
    def _has_not_null_constraint(self, column_def: exp.ColumnDef) -> bool:
        """Check if column has NOT NULL constraint."""
        if column_def.constraints:
            for constraint in column_def.constraints:
                if isinstance(constraint, exp.ColumnConstraint):
                    if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
                        # Primary keys are implicitly NOT NULL
                        return True
                    elif isinstance(constraint.kind, exp.NotNullColumnConstraint):
                        # Check the string representation to distinguish NULL vs NOT NULL
                        constraint_str = str(constraint).upper()
                        if constraint_str == "NOT NULL":
                            return True
                        # If it's just "NULL", then it's explicitly nullable
        return False
    
    def _extract_dependencies(self, select_stmt: exp.Select) -> Set[str]:
        """Extract table dependencies from SELECT statement."""
        dependencies = set()
        
        # Use find_all to get all table references
        for table in select_stmt.find_all(exp.Table):
            table_name = self._get_table_name(table)
            if table_name != "unknown":
                dependencies.add(table_name)
        
        return dependencies
    
    def _extract_column_lineage(self, select_stmt: exp.Select, view_name: str) -> tuple[List[ColumnLineage], List[ColumnSchema]]:
        """Extract column lineage from SELECT statement."""
        lineage = []
        output_columns = []
        
        if not select_stmt.expressions:
            return lineage, output_columns
        
        for i, select_expr in enumerate(select_stmt.expressions):
            if isinstance(select_expr, exp.Alias):
                # Aliased column: SELECT column AS alias
                output_name = str(select_expr.alias)
                source_expr = select_expr.this
            else:
                # Direct column reference or expression
                # For direct column references, extract just the column name
                if isinstance(select_expr, exp.Column):
                    output_name = str(select_expr.this)  # Just the column name, not table.column
                else:
                    output_name = str(select_expr)
                source_expr = select_expr
            
            # Create output column schema
            output_columns.append(ColumnSchema(
                name=output_name,
                data_type="unknown",  # Would need type inference
                nullable=True,
                ordinal=i
            ))
            
            # Extract lineage for this column
            col_lineage = self._analyze_expression_lineage(
                output_name, source_expr, select_stmt
            )
            lineage.append(col_lineage)
        
        return lineage, output_columns
    
    def _analyze_expression_lineage(self, output_name: str, expr: exp.Expression, context: exp.Select) -> ColumnLineage:
        """Analyze an expression to determine its lineage."""
        input_fields = []
        transformation_type = TransformationType.IDENTITY
        description = ""
        
        if isinstance(expr, exp.Column):
            # Simple column reference
            table_alias = str(expr.table) if expr.table else None
            column_name = str(expr.this)
            
            # Resolve table name from alias
            table_name = self._resolve_table_from_alias(table_alias, context)
            
            input_fields.append(ColumnReference(
                namespace="mssql://localhost/InfoTrackerDW",
                table_name=table_name,
                column_name=column_name
            ))
            # Create description like "OrderID from Orders.OrderID"
            table_simple = table_name.split('.')[-1] if '.' in table_name else table_name
            description = f"{output_name} from {table_simple}.{column_name}"
            
        elif isinstance(expr, exp.Cast):
            # CAST expression
            transformation_type = TransformationType.CAST
            inner_expr = expr.this
            target_type = str(expr.to).upper()
            
            if isinstance(inner_expr, exp.Column):
                table_alias = str(inner_expr.table) if inner_expr.table else None
                column_name = str(inner_expr.this)
                table_name = self._resolve_table_from_alias(table_alias, context)
                
                input_fields.append(ColumnReference(
                    namespace="mssql://localhost/InfoTrackerDW",
                    table_name=table_name,
                    column_name=column_name
                ))
                description = f"CAST({column_name} AS {target_type})"
            
        elif isinstance(expr, exp.Case):
            # CASE expression
            transformation_type = TransformationType.CASE
            
            # Extract columns referenced in CASE conditions and values
            for column_ref in expr.find_all(exp.Column):
                table_alias = str(column_ref.table) if column_ref.table else None
                column_name = str(column_ref.this)
                table_name = self._resolve_table_from_alias(table_alias, context)
                
                input_fields.append(ColumnReference(
                    namespace="mssql://localhost/InfoTrackerDW",
                    table_name=table_name,
                    column_name=column_name
                ))
            
            # Create a more detailed description for CASE expressions
            description = str(expr).replace('\n', ' ').replace('  ', ' ')
            
        else:
            # Other expressions - extract all column references
            transformation_type = TransformationType.EXPRESSION
            
            for column_ref in expr.find_all(exp.Column):
                table_alias = str(column_ref.table) if column_ref.table else None
                column_name = str(column_ref.this)
                table_name = self._resolve_table_from_alias(table_alias, context)
                
                input_fields.append(ColumnReference(
                    namespace="mssql://localhost/InfoTrackerDW",
                    table_name=table_name,
                    column_name=column_name
                ))
            
            description = f"Expression: {str(expr)}"
        
        return ColumnLineage(
            output_column=output_name,
            input_fields=input_fields,
            transformation_type=transformation_type,
            transformation_description=description
        )
    
    def _resolve_table_from_alias(self, alias: Optional[str], context: exp.Select) -> str:
        """Resolve actual table name from alias in SELECT context."""
        if not alias:
            # Try to find the single table in the query
            tables = list(context.find_all(exp.Table))
            if len(tables) == 1:
                return self._get_table_name(tables[0])
            return "unknown"
        
        # Look for alias in table references
        for table in context.find_all(exp.Table):
            if hasattr(table, 'alias') and str(table.alias) == alias:
                return self._get_table_name(table)
            # Sometimes the alias is in the parent expression
            if table.parent and hasattr(table.parent, 'alias') and str(table.parent.alias) == alias:
                return self._get_table_name(table)
        
        return alias  # Fallback to alias as table name

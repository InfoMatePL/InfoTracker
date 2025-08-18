"""
Unit tests for the SQL parser component.
"""
import pytest
import json
from pathlib import Path

from infotracker.parser import SqlParser
from infotracker.models import TransformationType


class TestSqlParser:
    """Test cases for SQL parser functionality."""

    def setup_method(self):
        """Set up test instance."""
        self.parser = SqlParser()

    def test_create_table_parsing(self, sql_content, expected_lineage):
        """Test parsing of CREATE TABLE statements."""
        # Test the customers table
        sql = sql_content["01_customers"]
        obj_info = self.parser.parse_sql_file(sql, "01_customers")
        
        assert obj_info.name == "STG.dbo.Customers"
        assert obj_info.object_type == "table"
        assert len(obj_info.schema.columns) == 4
        assert len(obj_info.lineage) == 0  # Tables don't have lineage
        assert len(obj_info.dependencies) == 0  # Tables don't have dependencies
        
        # Check column details
        columns = {col.name: col for col in obj_info.schema.columns}
        
        # CustomerID - Primary Key (not nullable)
        assert "CustomerID" in columns
        assert columns["CustomerID"].data_type == "int"
        assert columns["CustomerID"].nullable == False
        
        # CustomerName - NOT NULL
        assert "CustomerName" in columns
        assert columns["CustomerName"].data_type == "nvarchar(100)"
        assert columns["CustomerName"].nullable == False
        
        # Email - NULL allowed
        assert "Email" in columns
        assert columns["Email"].data_type == "nvarchar(255)"
        assert columns["Email"].nullable == True
        
        # SignupDate - NULL allowed
        assert "SignupDate" in columns
        assert columns["SignupDate"].data_type == "date"
        assert columns["SignupDate"].nullable == True

    def test_create_view_parsing(self, sql_content):
        """Test parsing of CREATE VIEW statements."""
        # Test the stg_orders view
        sql = sql_content["10_stg_orders"]
        obj_info = self.parser.parse_sql_file(sql, "10_stg_orders")
        
        assert obj_info.name == "STG.dbo.stg_orders"
        assert obj_info.object_type == "view"
        assert len(obj_info.schema.columns) == 4
        assert len(obj_info.lineage) == 4
        assert obj_info.dependencies == {"STG.dbo.Orders"}
        
        # Check lineage details
        lineage_by_column = {lin.output_column: lin for lin in obj_info.lineage}
        
        # OrderID - Identity transformation
        assert "OrderID" in lineage_by_column
        orderid_lineage = lineage_by_column["OrderID"]
        assert orderid_lineage.transformation_type == TransformationType.IDENTITY
        assert len(orderid_lineage.input_fields) == 1
        assert orderid_lineage.input_fields[0].table_name == "STG.dbo.Orders"
        assert orderid_lineage.input_fields[0].column_name == "OrderID"
        
        # CustomerID - Identity transformation
        assert "CustomerID" in lineage_by_column
        customerid_lineage = lineage_by_column["CustomerID"]
        assert customerid_lineage.transformation_type == TransformationType.IDENTITY
        
        # OrderDate - CAST transformation
        assert "OrderDate" in lineage_by_column
        orderdate_lineage = lineage_by_column["OrderDate"]
        assert orderdate_lineage.transformation_type == TransformationType.CAST
        assert "CAST" in orderdate_lineage.transformation_description.upper()
        
        # IsFulfilled - CASE transformation
        assert "IsFulfilled" in lineage_by_column
        isfulfilled_lineage = lineage_by_column["IsFulfilled"]
        assert isfulfilled_lineage.transformation_type == TransformationType.CASE
        assert len(isfulfilled_lineage.input_fields) == 1
        assert isfulfilled_lineage.input_fields[0].column_name == "Status"

    def test_dependency_extraction(self, sql_content):
        """Test extraction of table dependencies."""
        # Test a view with JOIN (if available)
        if "40_fct_sales" in sql_content:
            sql = sql_content["40_fct_sales"]
            obj_info = self.parser.parse_sql_file(sql, "40_fct_sales")
            
            # Should have dependencies on both tables in the JOIN
            assert len(obj_info.dependencies) >= 1
            # The exact dependencies depend on the SQL content

    def test_error_handling(self):
        """Test error handling for invalid SQL."""
        invalid_sql = "INVALID SQL STATEMENT;"
        obj_info = self.parser.parse_sql_file(invalid_sql, "test")
        
        # Should return an object with minimal information
        assert obj_info.name == "test"
        assert obj_info.object_type == "unknown"

    @pytest.mark.parametrize("sql_file", [
        "01_customers", "02_orders", "03_products", "04_order_items"
    ])
    def test_all_table_files(self, sql_content, sql_file):
        """Test parsing of all CREATE TABLE files."""
        if sql_file not in sql_content:
            pytest.skip(f"SQL file {sql_file} not found")
            
        sql = sql_content[sql_file]
        obj_info = self.parser.parse_sql_file(sql, sql_file)
        
        assert obj_info.object_type == "table"
        assert len(obj_info.schema.columns) > 0
        assert len(obj_info.lineage) == 0
        assert len(obj_info.dependencies) == 0

    @pytest.mark.parametrize("sql_file", [
        "10_stg_orders", "11_stg_order_items", "12_stg_customers"
    ])
    def test_staging_view_files(self, sql_content, sql_file):
        """Test parsing of staging view files."""
        if sql_file not in sql_content:
            pytest.skip(f"SQL file {sql_file} not found")
            
        sql = sql_content[sql_file]
        obj_info = self.parser.parse_sql_file(sql, sql_file)
        
        assert obj_info.object_type == "view"
        assert len(obj_info.schema.columns) > 0
        assert len(obj_info.lineage) > 0
        assert len(obj_info.dependencies) > 0

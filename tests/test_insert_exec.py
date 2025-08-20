"""
Test INSERT ... EXEC parsing functionality.
"""
import pytest
from src.infotracker.parser import SqlParser
from src.infotracker.models import TransformationType


class TestInsertExecParsing:
    """Test INSERT ... EXEC statement parsing."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.parser = SqlParser(dialect="tsql")
    
    def test_insert_exec_parsing_basic(self):
        """Test basic INSERT ... EXEC parsing."""
        sql = """
        INSERT INTO #temp_results
        EXEC dbo.GetCustomerData @param1 = 'value1', @param2 = 123
        """
        
        obj_info = self.parser.parse_sql_file(sql, object_hint="test_insert_exec")
        
        # Check basic object properties
        assert obj_info.name == "#temp_results"
        assert obj_info.object_type == "temp_table"
        assert "dbo.GetCustomerData" in obj_info.dependencies
        
        # Check schema has at least 2 columns as required
        assert len(obj_info.schema.columns) >= 2
        assert obj_info.schema.columns[0].name == "output_col_1"
        assert obj_info.schema.columns[1].name == "output_col_2"
        
        # Check lineage uses correct transformation type
        assert len(obj_info.lineage) >= 2
        for lineage_item in obj_info.lineage:
            assert lineage_item.transformation_type == TransformationType.EXEC
            assert len(lineage_item.input_fields) == 1
            assert lineage_item.input_fields[0].table_name == "dbo.GetCustomerData"
            assert lineage_item.input_fields[0].column_name == "*"
    
    def test_insert_exec_regular_table(self):
        """Test INSERT ... EXEC into regular table (not temp)."""
        sql = """
        INSERT INTO staging.customer_data
        EXEC warehouse.sp_transform_customers
        """
        
        obj_info = self.parser.parse_sql_file(sql, object_hint="test_regular_insert_exec")
        
        # Check it's treated as regular table, not temp table
        assert obj_info.name == "staging.customer_data"
        assert obj_info.object_type == "table"  # not temp_table
        assert "warehouse.sp_transform_customers" in obj_info.dependencies
    
    def test_insert_exec_with_params(self):
        """Test INSERT ... EXEC with multiple parameters."""
        sql = """
        INSERT INTO #results 
        EXEC dbo.ComplexProcedure 
            @StartDate = '2023-01-01',
            @EndDate = '2023-12-31',
            @Category = 'Premium'
        """
        
        obj_info = self.parser.parse_sql_file(sql, object_hint="test_params")
        
        assert obj_info.name == "#results"
        assert "dbo.ComplexProcedure" in obj_info.dependencies
        assert len(obj_info.lineage) >= 2
        
        # Verify lineage description includes table and procedure names
        for lineage_item in obj_info.lineage:
            assert "INSERT INTO #results EXEC dbo.ComplexProcedure" in lineage_item.transformation_description

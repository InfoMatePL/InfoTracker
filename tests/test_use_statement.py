"""
Tests for USE statement parsing functionality.
"""
import pytest
from infotracker.parser import SqlParser


def test_use_statement_colon_format():
    """Test that USE :DBNAME: format sets the database context correctly."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE :WarehouseDB:

INSERT INTO #temp EXEC GetCustomerData
"""
    
    # Parse the content
    result = parser._try_insert_exec_fallback(content)
    
    # Check that the database context was set
    assert parser.current_database == "WarehouseDB"
    
    # Check that the dependency uses the correct database
    assert result is not None
    assert "WarehouseDB.dbo.GetCustomerData" in result.dependencies


def test_use_statement_brackets_format():
    """Test that USE [database] format sets the database context correctly."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [ProductionDB]

INSERT INTO orders EXEC GetOrderData
"""
    
    # Parse the content
    result = parser._try_insert_exec_fallback(content)
    
    # Check that the database context was set
    assert parser.current_database == "ProductionDB"
    
    # Check that the dependency uses the correct database
    assert result is not None
    assert "ProductionDB.dbo.GetOrderData" in result.dependencies


def test_use_statement_simple_format():
    """Test that USE database format (no brackets) sets the database context correctly."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE TestDB

INSERT INTO #temp EXEC GetTestData
"""
    
    # Parse the content
    result = parser._try_insert_exec_fallback(content)
    
    # Check that the database context was set
    assert parser.current_database == "TestDB"
    
    # Check that the dependency uses the correct database
    assert result is not None
    assert "TestDB.dbo.GetTestData" in result.dependencies


def test_no_use_statement_uses_default():
    """Test that files without USE statements use the default database."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
INSERT INTO #temp EXEC GetData
"""
    
    # Parse the content
    result = parser._try_insert_exec_fallback(content)
    
    # Check that the default database is used
    assert parser.current_database == "InfoTrackerDW"
    
    # Check that the dependency uses the default database
    assert result is not None
    assert "InfoTrackerDW.dbo.GetData" in result.dependencies


def test_use_statement_with_comments():
    """Test that USE statements work correctly with comments."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
-- This is a comment
USE :ProductionDB:
-- Another comment

INSERT INTO #temp EXEC GetProductionData
"""
    
    # Parse the content
    result = parser._try_insert_exec_fallback(content)
    
    # Check that the database context was set
    assert parser.current_database == "ProductionDB"
    
    # Check that the dependency uses the correct database
    assert result is not None
    assert "ProductionDB.dbo.GetProductionData" in result.dependencies


def test_full_table_name_generation():
    """Test that _get_full_table_name works correctly with database context."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    parser.current_database = "WarehouseDB"
    
    # Test simple table name
    assert parser._get_full_table_name("orders") == "WarehouseDB.dbo.orders"
    
    # Test schema.table format
    assert parser._get_full_table_name("sales.orders") == "WarehouseDB.sales.orders"
    
    # Test full database.schema.table format (should remain unchanged)
    assert parser._get_full_table_name("OtherDB.sales.orders") == "OtherDB.sales.orders"
    
    # Test with no current database (should use default)
    parser.current_database = None
    assert parser._get_full_table_name("orders") == "InfoTrackerDW.dbo.orders"


def test_multiple_files_reset_database():
    """Test that parsing multiple files resets the database context correctly."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    # First file with USE statement
    content1 = """
USE :WarehouseDB:
INSERT INTO #temp1 EXEC GetData1
"""
    
    obj1 = parser.parse_sql_file(content1)
    assert parser.current_database == "WarehouseDB"
    
    # Second file without USE statement should reset to default
    content2 = """
INSERT INTO #temp2 EXEC GetData2
"""
    
    obj2 = parser.parse_sql_file(content2)
    assert parser.current_database == "InfoTrackerDW"


def test_extract_database_from_use_statement():
    """Test the database extraction method directly."""
    parser = SqlParser()
    
    # Test colon format
    content1 = "USE :TestDB:\nSELECT * FROM table"
    assert parser._extract_database_from_use_statement(content1) == "TestDB"
    
    # Test bracket format
    content2 = "USE [ProductionDB]\nSELECT * FROM table"
    assert parser._extract_database_from_use_statement(content2) == "ProductionDB"
    
    # Test simple format
    content3 = "USE StagingDB\nSELECT * FROM table"
    assert parser._extract_database_from_use_statement(content3) == "StagingDB"
    
    # Test no USE statement
    content4 = "SELECT * FROM table"
    assert parser._extract_database_from_use_statement(content4) is None
    
    # Test USE statement with comments
    content5 = """
-- Comment
USE :DevDB:
-- Another comment
SELECT * FROM table
"""
    assert parser._extract_database_from_use_statement(content5) == "DevDB"

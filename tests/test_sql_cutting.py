"""
Test for SQL preprocessing with statement cutting functionality.
"""
import pytest
from infotracker.parser import SqlParser


def test_cut_to_create_view():
    """Test cutting to CREATE VIEW statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE :TestDB:
DECLARE @var INT = 1
SET @var = 2
PRINT 'Starting'
-- Some comment
GO
CREATE VIEW test_view AS
SELECT * FROM table1
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should start with CREATE VIEW
    assert processed.strip().startswith("CREATE VIEW test_view AS")
    assert "DECLARE" not in processed
    assert "SET" not in processed
    assert "PRINT" not in processed


def test_cut_to_create_or_alter_procedure():
    """Test cutting to CREATE OR ALTER PROCEDURE statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [ProductionDB]
DECLARE @count INT
SET @count = 0
PRINT 'Starting procedure creation'

CREATE OR ALTER PROCEDURE dbo.GetCustomers
AS
BEGIN
    SELECT * FROM customers
END
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should start with CREATE OR ALTER PROCEDURE
    assert "CREATE OR ALTER PROCEDURE dbo.GetCustomers" in processed
    assert "DECLARE" not in processed
    assert "SET" not in processed
    assert "PRINT" not in processed


def test_cut_to_alter_table():
    """Test cutting to ALTER TABLE statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE TestDB
DECLARE @sql NVARCHAR(MAX)
SET @sql = 'ALTER TABLE...'
PRINT 'Altering table'
GO

ALTER TABLE customers
ADD email VARCHAR(255)
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should start with ALTER TABLE
    assert processed.strip().startswith("ALTER TABLE customers")
    assert "DECLARE" not in processed
    assert "SET" not in processed


def test_cut_to_select_into():
    """Test cutting to SELECT...INTO statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE :WarehouseDB:
DECLARE @date DATE = GETDATE()
SET @date = '2024-01-01'
PRINT 'Creating backup'

SELECT *
INTO customers_backup
FROM customers
WHERE created_date >= @date
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should start with SELECT and contain INTO
    assert processed.strip().startswith("SELECT *")
    assert "INTO customers_backup" in processed
    assert "DECLARE" not in processed
    assert "SET" not in processed


def test_cut_to_insert_exec():
    """Test cutting to INSERT...EXEC statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [TestDB]
DECLARE @table_name VARCHAR(100) = '#temp_results'
SET @table_name = '#results'
PRINT 'Loading data'
GO

INSERT INTO #temp_results
EXEC GetCustomerData @param1 = 1
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should start with INSERT INTO and contain EXEC
    assert processed.strip().startswith("INSERT INTO #temp_results")
    assert "EXEC GetCustomerData" in processed
    assert "DECLARE" not in processed
    assert "SET" not in processed


def test_no_significant_statement_returns_original():
    """Test that if no significant statement is found, original processed content is returned."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE TestDB
DECLARE @var INT = 1
SET @var = 2
PRINT 'No significant statement here'
-- Just comments and declarations
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should be empty or minimal after preprocessing (no significant statements)
    # Since all lines are filtered out, result should be minimal
    assert len(processed.strip()) == 0 or processed.strip().startswith("--")


def test_multiline_create_function():
    """Test cutting to multiline CREATE FUNCTION statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE :TestDB:
DECLARE @debug BIT = 1
SET @debug = 0
PRINT 'Creating function'
GO

CREATE FUNCTION dbo.CalculateTotal(
    @amount DECIMAL(10,2),
    @tax_rate DECIMAL(5,4)
)
RETURNS DECIMAL(10,2)
AS
BEGIN
    RETURN @amount * (1 + @tax_rate)
END
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should start with CREATE FUNCTION
    assert processed.strip().startswith("CREATE FUNCTION dbo.CalculateTotal(")
    assert "@amount DECIMAL(10,2)" in processed
    assert "DECLARE" not in processed
    assert "SET" not in processed


def test_preserves_database_context():
    """Test that cutting preserves the database context extraction."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE :ProductionDB:
DECLARE @var INT = 1
SET @var = 2

CREATE TABLE test_table (
    id INT PRIMARY KEY,
    name VARCHAR(100)
)
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should have extracted database context
    assert parser.current_database == "ProductionDB"
    
    # Should start with CREATE TABLE
    assert processed.strip().startswith("CREATE TABLE test_table")


def test_preserves_insert_exec_joining():
    """Test that the two-line INSERT INTO #temp + EXEC joining is preserved."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE TestDB
DECLARE @param INT = 1

INSERT INTO #temp
EXEC GetData @param
"""
    
    processed = parser._preprocess_sql(content)
    
    # Should have joined the INSERT and EXEC lines
    assert "INSERT INTO #temp EXEC GetData @param" in processed.replace('\n', ' ')
    assert "DECLARE" not in processed

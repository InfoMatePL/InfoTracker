"""
Tests for USE statement with CREATE TABLE parsing.
"""
import pytest
from infotracker.parser import SqlParser


def test_create_table_with_use_bracket_format():
    """Test that CREATE TABLE respects USE [database] statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [EDW_CORE]

CREATE TABLE dbo.TrialBalance_asefl_BV (
    ID INT NOT NULL,
    Amount DECIMAL(18,2),
    Description NVARCHAR(100)
)
"""
    
    result = parser.parse_sql_file(content, object_hint="TrialBalance_asefl_BV")
    
    # Verify database context was set
    assert parser.current_database == "EDW_CORE"
    
    # Verify namespace uses EDW_CORE, not InfoTrackerDW
    assert result.schema.namespace == "mssql://localhost/EDW_CORE"
    
    # Verify table name
    assert result.schema.name == "dbo.TrialBalance_asefl_BV"


def test_create_table_with_use_colon_format():
    """Test that CREATE TABLE respects USE :database: statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE :STG:

CREATE TABLE dbo.StagingTable (
    ID INT NOT NULL,
    Data NVARCHAR(MAX)
)
"""
    
    result = parser.parse_sql_file(content, object_hint="StagingTable")
    
    # Verify database context was set
    assert parser.current_database == "STG"
    
    # Verify namespace uses STG, not InfoTrackerDW
    assert result.schema.namespace == "mssql://localhost/STG"
    
    # Verify table name
    assert result.schema.name == "dbo.StagingTable"


def test_create_table_without_use_uses_default():
    """Test that CREATE TABLE without USE statement uses default database."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
CREATE TABLE dbo.TestTable (
    ID INT NOT NULL,
    Name NVARCHAR(50)
)
"""
    
    result = parser.parse_sql_file(content, object_hint="TestTable")
    
    # Verify default database is used
    assert parser.current_database == "InfoTrackerDW"
    
    # Verify namespace uses InfoTrackerDW
    assert result.schema.namespace == "mssql://localhost/INFOTRACKERDW"
    
    # Verify table name
    assert result.schema.name == "dbo.TestTable"


def test_create_table_with_explicit_database_overrides_use():
    """Test that explicit database in CREATE TABLE overrides USE statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [EDW_CORE]

CREATE TABLE OtherDB.dbo.SpecialTable (
    ID INT NOT NULL
)
"""
    
    result = parser.parse_sql_file(content, object_hint="SpecialTable")
    
    # Verify USE set the context
    assert parser.current_database == "EDW_CORE"
    
    # But explicit DB in CREATE TABLE should override
    # The table should be in OtherDB namespace, not EDW_CORE
    assert result.schema.namespace == "mssql://localhost/OTHERDB"
    
    # Verify table name
    assert result.schema.name == "dbo.SpecialTable"


def test_create_table_string_fallback_with_use():
    """Test string-based CREATE TABLE parser with USE statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [METRICS_CORE]

CREATE TABLE dbo.MetricsTable (
    MetricID INT NOT NULL,
    MetricValue DECIMAL(10,2),
    MetricName NVARCHAR(100)
)
"""
    
    # Force string-based parsing
    result = parser._parse_create_table_string(content, object_hint="MetricsTable")
    
    # Verify namespace uses METRICS_CORE from USE statement
    assert result.schema.namespace == "mssql://localhost/METRICS_CORE"
    
    # Verify table name
    assert result.schema.name == "dbo.MetricsTable"


def test_multiple_tables_different_databases():
    """Test parsing multiple tables from different databases maintains correct context."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    # First file with EDW_CORE
    content1 = """
USE [EDW_CORE]

CREATE TABLE dbo.Table1 (
    ID INT NOT NULL
)
"""
    
    result1 = parser.parse_sql_file(content1, object_hint="Table1")
    assert result1.schema.namespace == "mssql://localhost/EDW_CORE"
    assert parser.current_database == "EDW_CORE"
    
    # Second file with STG (should reset)
    content2 = """
USE [STG]

CREATE TABLE dbo.Table2 (
    ID INT NOT NULL
)
"""
    
    result2 = parser.parse_sql_file(content2, object_hint="Table2")
    assert result2.schema.namespace == "mssql://localhost/STG"
    assert parser.current_database == "STG"
    
    # Third file without USE (should use default)
    content3 = """
CREATE TABLE dbo.Table3 (
    ID INT NOT NULL
)
"""
    
    result3 = parser.parse_sql_file(content3, object_hint="Table3")
    assert result3.schema.namespace == "mssql://localhost/INFOTRACKERDW"
    assert parser.current_database == "InfoTrackerDW"


def test_case_insensitive_use_statement():
    """Test that USE statement is case-insensitive."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
use [edw_core]

CREATE TABLE dbo.CaseTestTable (
    ID INT NOT NULL
)
"""
    
    result = parser.parse_sql_file(content, object_hint="CaseTestTable")
    
    # Verify database context was set (preserving original case)
    assert parser.current_database == "edw_core"
    
    # Namespace should be uppercase (canonical form)
    assert result.schema.namespace == "mssql://localhost/EDW_CORE"

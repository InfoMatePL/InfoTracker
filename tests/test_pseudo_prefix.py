"""
Tests for pseudo prefix handling in object names (Table., View., StoredProcedure., etc.)
"""
import pytest
from infotracker.parser import SqlParser


def test_table_prefix_ignored_in_object_hint():
    """Test that 'Table.' prefix in object_hint is treated as pseudo keyword, not database."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [EDW_CORE]

CREATE TABLE dbo.TrialBalance_asefa_BV (
    ID INT NOT NULL,
    Amount DECIMAL(18,2)
)
"""
    
    # Simulate file name: Table.dbo.TrialBalance_asefa_BV.sql
    result = parser.parse_sql_file(content, object_hint="Table.dbo.TrialBalance_asefa_BV")
    
    # Should use EDW_CORE from USE statement, not "Table" as database
    assert result.schema.namespace == "mssql://localhost/EDW_CORE"
    assert result.schema.name == "dbo.TrialBalance_asefa_BV"


def test_storedprocedure_prefix_ignored():
    """Test that 'StoredProcedure.' prefix is treated as pseudo keyword."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [EDW_CORE]

CREATE PROCEDURE dbo.update_TrialBalance AS
BEGIN
    SELECT 1
END
"""
    
    # Simulate file name: StoredProcedure.dbo.update_TrialBalance.sql
    result = parser.parse_sql_file(content, object_hint="StoredProcedure.dbo.update_TrialBalance")
    
    # Should use EDW_CORE from USE statement
    assert result.schema.namespace == "mssql://localhost/EDW_CORE"
    assert result.schema.name == "dbo.update_TrialBalance"


def test_view_prefix_already_worked():
    """Test that 'View.' prefix continues to work (was already in pseudo set)."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [STG]

CREATE VIEW dbo.AccountBalance_asefa_TBH AS
SELECT 1 AS ID
"""
    
    result = parser.parse_sql_file(content, object_hint="View.dbo.AccountBalance_asefa_TBH")
    
    # Should use STG from USE statement
    assert result.schema.namespace == "mssql://localhost/STG"
    assert result.schema.name == "dbo.AccountBalance_asefa_TBH"


def test_no_use_statement_with_table_prefix():
    """Test that without USE statement, uses default database (not 'Table' as DB)."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
CREATE TABLE dbo.SomeTable (
    ID INT NOT NULL
)
"""
    
    result = parser.parse_sql_file(content, object_hint="Table.dbo.SomeTable")
    
    # Should use default InfoTrackerDW, not "Table"
    assert result.schema.namespace == "mssql://localhost/INFOTRACKERDW"
    assert result.schema.name == "dbo.SomeTable"


def test_mixed_case_table_prefix():
    """Test that pseudo keywords are case-insensitive."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [METRICS_CORE]

CREATE TABLE dbo.MetricsData (
    ID INT
)
"""
    
    # Mixed case: "table" instead of "Table"
    result = parser.parse_sql_file(content, object_hint="table.dbo.MetricsData")
    
    assert result.schema.namespace == "mssql://localhost/METRICS_CORE"
    assert result.schema.name == "dbo.MetricsData"


def test_explicit_db_overrides_use_even_with_pseudo():
    """Test that explicit DB in SQL overrides USE statement."""
    parser = SqlParser()
    parser.set_default_database("InfoTrackerDW")
    
    content = """
USE [EDW_CORE]

CREATE TABLE OtherDB.dbo.SpecialTable (
    ID INT
)
"""
    
    result = parser.parse_sql_file(content, object_hint="Table.dbo.SpecialTable")
    
    # Explicit DB in CREATE TABLE should win
    assert result.schema.namespace == "mssql://localhost/OTHERDB"
    assert result.schema.name == "dbo.SpecialTable"

"""
Unit tests for SQL preprocessing and INSERT EXEC fallback parsing.
"""
import pytest
from infotracker.parser import SqlParser
from infotracker.models import TransformationType


class TestSqlPreprocessing:
    """Test SQL preprocessing functionality."""
    
    def setup_method(self):
        """Set up test parser."""
        self.parser = SqlParser()
    
    def test_remove_control_statements(self):
        """Test removal of DECLARE, SET, PRINT statements."""
        sql = """
DECLARE @var INT = 1;
SET @another = 'test';
PRINT 'Hello world';
SELECT * FROM table1;
"""
        result = self.parser._preprocess_sql(sql)
        
        assert "DECLARE" not in result
        assert "SET" not in result
        assert "PRINT" not in result
        assert "SELECT * FROM table1" in result
    
    def test_remove_drop_temp_table(self):
        """Test removal of temp table DROP statements."""
        sql = """
IF OBJECT_ID('tempdb..#temp') IS NOT NULL DROP TABLE #temp;
DROP TABLE #another_temp;
CREATE TABLE #temp (id INT);
"""
        result = self.parser._preprocess_sql(sql)
        
        assert "IF OBJECT_ID" not in result
        assert "DROP TABLE #another_temp" not in result
        assert "CREATE TABLE #temp" in result
    
    def test_join_insert_exec_lines(self):
        """Test joining of two-line INSERT INTO #temp + EXEC patterns."""
        sql = """
INSERT INTO #customer_metrics
EXEC dbo.usp_procedure;
"""
        result = self.parser._preprocess_sql(sql)
        
        assert "INSERT INTO #customer_metrics EXEC dbo.usp_procedure" in result
        # Should be on a single line now
        lines = [line.strip() for line in result.split('\n') if line.strip()]
        insert_exec_lines = [line for line in lines if 'INSERT INTO #customer_metrics EXEC' in line]
        assert len(insert_exec_lines) == 1


class TestInsertExecFallback:
    """Test INSERT EXEC fallback parsing."""
    
    def setup_method(self):
        """Set up test parser."""
        self.parser = SqlParser()
    
    def test_fallback_detects_insert_exec_pattern(self):
        """Test that fallback detects INSERT INTO #temp EXEC pattern."""
        sql = """
INSERT INTO #customer_metrics
EXEC dbo.usp_customer_metrics_dataset;
"""
        result = self.parser._try_insert_exec_fallback(sql)
        
        assert result is not None
        assert result.name == "#customer_metrics"
        assert result.object_type == "temp_table"
        assert result.schema.namespace == "tempdb"
    
    def test_fallback_sets_dependencies(self):
        """Test that fallback sets correct dependencies."""
        sql = "INSERT INTO #temp EXEC dbo.my_procedure;"
        result = self.parser._try_insert_exec_fallback(sql)
        
        assert result is not None
        assert "dbo.my_procedure" in result.dependencies
    
    def test_fallback_creates_lineage(self):
        """Test that fallback creates correct lineage."""
        sql = "INSERT INTO #test EXEC schema.proc_name;"
        result = self.parser._try_insert_exec_fallback(sql)
        
        assert result is not None
        assert len(result.lineage) == 2  # Two placeholder columns
        
        for lineage in result.lineage:
            assert lineage.transformation_type == TransformationType.EXEC
            assert len(lineage.input_fields) == 1
            assert lineage.input_fields[0].table_name == "schema.proc_name"
            assert lineage.input_fields[0].column_name == "*"
            assert "INSERT INTO #test EXEC schema.proc_name" in lineage.transformation_description
    
    def test_fallback_creates_placeholder_columns(self):
        """Test that fallback creates placeholder columns."""
        sql = "INSERT INTO #metrics EXEC get_metrics;"
        result = self.parser._try_insert_exec_fallback(sql)
        
        assert result is not None
        assert len(result.schema.columns) == 2
        
        col1, col2 = result.schema.columns
        assert col1.name == "output_col_1"
        assert col1.data_type == "unknown"
        assert col1.nullable is True
        assert col1.ordinal == 0
        
        assert col2.name == "output_col_2"
        assert col2.ordinal == 1
    
    def test_fallback_ignores_non_matching_sql(self):
        """Test that fallback returns None for non-matching SQL."""
        sql = "SELECT * FROM table1;"
        result = self.parser._try_insert_exec_fallback(sql)
        
        assert result is None


class TestFullParsingWithPreprocessing:
    """Test full parsing integration with preprocessing."""
    
    def setup_method(self):
        """Set up test parser."""
        self.parser = SqlParser()
    
    def test_parse_sql_file_with_preprocessing(self):
        """Test that parse_sql_file uses preprocessing and fallback."""
        sql = """
DECLARE @test INT = 1;
SET NOCOUNT ON;
PRINT 'Starting';

IF OBJECT_ID('tempdb..#temp') IS NOT NULL DROP TABLE #temp;

INSERT INTO #customer_data
EXEC dbo.get_customer_data;
"""
        result = self.parser.parse_sql_file(sql, "test_file")
        
        # Should use fallback to parse the INSERT EXEC
        assert result.name == "#customer_data"
        assert result.object_type == "temp_table"
        assert result.schema.namespace == "tempdb"
        assert "dbo.get_customer_data" in result.dependencies
        assert len(result.lineage) == 2
        
        # Lineage should use EXEC transformation
        for lineage in result.lineage:
            assert lineage.transformation_type == TransformationType.EXEC

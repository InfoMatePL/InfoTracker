"""
Test preprocessing and fallback functionality.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from infotracker.parser import SqlParser
from infotracker.models import TransformationType

def test_preprocessing():
    """Test SQL preprocessing functionality."""
    print("Testing SQL preprocessing...")
    
    parser = SqlParser()
    
    # Test SQL with control statements and two-line INSERT EXEC
    test_sql = """
DECLARE @CustomerID INT = 1;
SET @StartDate = '2024-01-01';
PRINT 'Starting process';

IF OBJECT_ID('tempdb..#customer_metrics') IS NOT NULL DROP TABLE #customer_metrics;
DROP TABLE #temp_test;

INSERT INTO #customer_metrics
EXEC dbo.usp_customer_metrics_dataset;

SELECT * FROM #customer_metrics;
"""
    
    preprocessed = parser._preprocess_sql(test_sql)
    print("✓ Preprocessed SQL:")
    print(preprocessed)
    
    # Verify control lines are removed
    assert "DECLARE" not in preprocessed
    assert "SET" not in preprocessed
    assert "PRINT" not in preprocessed
    assert "IF OBJECT_ID" not in preprocessed
    assert "DROP TABLE #temp_test" not in preprocessed
    
    # Verify INSERT EXEC is joined
    assert "INSERT INTO #customer_metrics EXEC dbo.usp_customer_metrics_dataset" in preprocessed
    print("✓ All preprocessing rules applied correctly")

def test_insert_exec_fallback():
    """Test INSERT INTO #temp EXEC fallback parsing."""
    print("\nTesting INSERT EXEC fallback...")
    
    parser = SqlParser()
    
    # Test SQL that would fail normal parsing but should work with fallback
    test_sql = """
-- Some comments
INSERT INTO #customer_metrics
EXEC dbo.usp_customer_metrics_dataset;
-- More content
"""
    
    result = parser._try_insert_exec_fallback(test_sql)
    
    assert result is not None, "Fallback should detect INSERT EXEC pattern"
    assert result.name == "#customer_metrics", f"Expected temp table name, got {result.name}"
    assert result.object_type == "temp_table", f"Expected temp_table type, got {result.object_type}"
    assert result.schema.namespace == "tempdb", f"Expected tempdb namespace, got {result.schema.namespace}"
    assert "dbo.usp_customer_metrics_dataset" in result.dependencies, "Should depend on the procedure"
    
    # Check lineage
    assert len(result.lineage) == 2, f"Expected 2 lineage entries, got {len(result.lineage)}"
    for lineage in result.lineage:
        assert lineage.transformation_type == TransformationType.EXEC
        assert len(lineage.input_fields) == 1
        assert lineage.input_fields[0].table_name == "dbo.usp_customer_metrics_dataset"
        assert lineage.input_fields[0].column_name == "*"
    
    print("✓ Fallback parsing works correctly")
    print(f"✓ Temp table: {result.name} (namespace: {result.schema.namespace})")
    print(f"✓ Dependencies: {result.dependencies}")
    print(f"✓ Lineage entries: {len(result.lineage)}")

def test_full_parsing():
    """Test full parsing with the new functionality."""
    print("\nTesting full parse_sql_file with preprocessing...")
    
    parser = SqlParser()
    
    # Test SQL that requires preprocessing and fallback
    test_sql = """
DECLARE @test INT = 1;
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#temp') IS NOT NULL DROP TABLE #temp;

INSERT INTO #customer_metrics
EXEC dbo.usp_customer_metrics_dataset;
"""
    
    result = parser.parse_sql_file(test_sql, "test_file")
    
    assert result.name == "#customer_metrics", "Should parse as temp table"
    assert result.object_type == "temp_table"
    assert result.schema.namespace == "tempdb"
    assert "dbo.usp_customer_metrics_dataset" in result.dependencies
    
    print("✓ Full parsing with preprocessing works correctly")

if __name__ == "__main__":
    print("Running preprocessing and fallback tests...\n")
    
    try:
        test_preprocessing()
        test_insert_exec_fallback()
        test_full_parsing()
        print("\n✅ All tests passed! Preprocessing and fallback implementation is working correctly.")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

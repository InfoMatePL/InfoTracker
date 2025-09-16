"""
Test INSERT ... EXEC parsing functionality.
"""
import pytest, re
from src.infotracker.parser import SqlParser
from src.infotracker.models import TransformationType


def _canon_exec_desc(desc: str) -> str:
    if not isinstance(desc, str):
        return desc
    # tempdb..#tmp -> #tmp
    out = re.sub(r'(?i)\btempdb\.\.(#\w+)', r'\1', desc)
    # EXEC DB.schema.proc -> EXEC schema.proc
    out = re.sub(r'(?i)\bEXEC\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+\.[A-Za-z0-9_]+)', r'EXEC \2', out)
    return out

def _canon_table_name(name: str) -> str:
    """
    Normalizuje nazwy tabel na potrzeby asercji testów:
    - usuwa 'tempdb..' z przodu (#temp tabelki),
    - sprowadza 'DB.schema.table' do 'schema.table'.
    """
    if not isinstance(name, str):
        return name
    # usuń tempdb.. (dla temp)
    name = re.sub(r'(?i)^tempdb\.\.', '', name)
    # zbij DB.schema.table -> schema.table
    parts = name.split('.')
    if len(parts) >= 3:
        return '.'.join(parts[-2:])
    return name

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
        assert _canon_table_name(obj_info.name) == "#temp_results"
        assert obj_info.object_type == "temp_table"
        assert "dbo.GetCustomerData" in {_canon_table_name(d) for d in obj_info.dependencies}
        
        # fallback nie zna liczby kolumn – wymagamy co najmniej 1
        assert len(obj_info.schema.columns) >= 1
        names = [c.name for c in obj_info.schema.columns]
        assert names[0].startswith("output_col_")
        if len(names) >= 2:
            assert names[1].startswith("output_col_")
        # Check lineage uses correct transformation type
        assert len(obj_info.lineage) >= 1
        for lineage_item in obj_info.lineage:
            assert lineage_item.transformation_type == TransformationType.EXEC
            assert len(lineage_item.input_fields) == 1
            assert _canon_table_name(lineage_item.input_fields[0].table_name) == "dbo.GetCustomerData"
            assert lineage_item.input_fields[0].column_name == "*"
    
    def test_insert_exec_regular_table(self):
        """Test INSERT ... EXEC into regular table (not temp)."""
        sql = """
        INSERT INTO staging.customer_data
        EXEC warehouse.sp_transform_customers
        """
        
        obj_info = self.parser.parse_sql_file(sql, object_hint="test_regular_insert_exec")
        
        # Check it's treated as regular table, not temp table
        assert _canon_table_name(obj_info.name) == "staging.customer_data"
        assert obj_info.object_type == "table"  # not temp_table
        assert "warehouse.sp_transform_customers" in {_canon_table_name(d) for d in obj_info.dependencies}
    
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
        
        assert _canon_table_name(obj_info.name) == "#results"
        assert "dbo.ComplexProcedure" in {_canon_table_name(d) for d in obj_info.dependencies}
        assert len(obj_info.lineage) >= 1
        
        # Verify lineage description includes table and procedure names
        for lineage_item in obj_info.lineage:
            assert "INSERT INTO #results EXEC dbo.ComplexProcedure" in _canon_exec_desc(lineage_item.transformation_description)
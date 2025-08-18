"""
Tests for wildcard selector functionality.
"""
import pytest
from infotracker.models import ColumnGraph, ColumnNode, ColumnEdge, TransformationType


class TestWildcardSelectors:
    """Test wildcard selector functionality."""
    
    def setup_method(self):
        """Set up test graph."""
        self.graph = ColumnGraph()
        
        # Add some sample nodes and edges
        nodes = [
            ColumnNode("mssql://localhost/InfoTrackerDW", "STG.dbo.Customers", "CustomerID"),
            ColumnNode("mssql://localhost/InfoTrackerDW", "STG.dbo.Customers", "CustomerName"),
            ColumnNode("mssql://localhost/InfoTrackerDW", "STG.dbo.Orders", "OrderID"),
            ColumnNode("mssql://localhost/InfoTrackerDW", "STG.dbo.Orders", "CustomerID"),
            ColumnNode("mssql://localhost/InfoTrackerDW", "STG.dbo.Orders", "Revenue"),
            ColumnNode("mssql://localhost/InfoTrackerDW", "INFOMART.dbo.fct_sales", "Revenue"),
            ColumnNode("mssql://localhost/InfoTrackerDW", "INFOMART.dbo.fct_sales", "CustomerID"),
            ColumnNode("mssql://localhost/InfoTrackerDW", "INFOMART.dbo.dim_customer", "customer_id"),
        ]
        
        # Add edges
        edges = [
            ColumnEdge(nodes[4], nodes[5], TransformationType.IDENTITY, "Revenue lineage"),  # Orders.Revenue -> fct_sales.Revenue
            ColumnEdge(nodes[3], nodes[6], TransformationType.IDENTITY, "CustomerID lineage"),  # Orders.CustomerID -> fct_sales.CustomerID
            ColumnEdge(nodes[0], nodes[7], TransformationType.IDENTITY, "Customer lineage"),  # Customers.CustomerID -> dim_customer.customer_id
        ]
        
        for edge in edges:
            self.graph.add_edge(edge)
    
    def test_table_wildcard_exact_match(self):
        """Test table wildcard with exact schema.table match."""
        # Test STG.dbo.Orders.*
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW.STG.dbo.Orders.*")
        column_names = [node.column_name for node in results]
        
        assert len(results) == 3
        assert "OrderID" in column_names
        assert "CustomerID" in column_names
        assert "Revenue" in column_names
    
    def test_table_wildcard_different_schema(self):
        """Test table wildcard with different schema."""
        # Test INFOMART.dbo.fct_sales.*
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW.INFOMART.dbo.fct_sales.*")
        column_names = [node.column_name for node in results]
        
        assert len(results) == 2
        assert "Revenue" in column_names
        assert "CustomerID" in column_names
    
    def test_column_wildcard_contains_case_insensitive(self):
        """Test column wildcard with case-insensitive contains matching."""
        # Test ..revenue (should match Revenue columns case-insensitively)
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW..revenue")
        column_names = [node.column_name for node in results]
        
        assert len(results) >= 1
        lower_cols = [name.lower() for name in column_names]
        assert any("revenue" in c for c in lower_cols)
    
    def test_column_wildcard_customer_pattern(self):
        """Test column wildcard with customer pattern."""
        # Test ..customer (should match CustomerID, CustomerName, customer_id)
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW..customer")
        column_names = [node.column_name for node in results]
        
        assert len(results) >= 1
        lower_cols = [name.lower() for name in column_names]
        assert any("customer" in c for c in lower_cols)
    
    def test_column_wildcard_with_namespace_prefix(self):
        """Test column wildcard with specific namespace prefix."""
        # Test mssql://localhost/InfoTrackerDW.STG..customer
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW.STG..customer")
        
        assert len(results) >= 1
        # Should only match columns from STG schema
        for node in results:
            assert "STG." in node.table_name
        
        # Check that results contain "customer" in column name (case-insensitive)
        column_names = [node.column_name for node in results]
        lower_cols = [name.lower() for name in column_names]
        assert any("customer" in c for c in lower_cols)
    
    def test_column_wildcard_fnmatch_pattern(self):
        """Test column wildcard with fnmatch patterns."""
        # Test ..Customer* (should match CustomerID, CustomerName)
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW..Customer*")
        column_names = [node.column_name for node in results]
        
        # Should match Customer* pattern case-insensitively
        assert len(results) >= 1
        lower_cols = [name.lower() for name in column_names]
        assert any("customer" in c for c in lower_cols)
    
    def test_no_wildcard_matches(self):
        """Test wildcard that matches nothing."""
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW..nonexistent")
        assert len(results) == 0
    
    def test_empty_wildcard_pattern(self):
        """Test empty or invalid wildcard patterns."""
        # Empty column wildcard should yield 0 matches
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW..")
        assert len(results) == 0
        
        # Empty column wildcard without namespace should also yield 0 matches
        results = self.graph.find_columns_wildcard("..")
        assert len(results) == 0

    def test_column_wildcard_contains_customer(self):
        """Test column wildcard contains semantics for customer."""
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW..customer")
        assert len(results) >= 1
        
        # Extract column names from all results
        column_names = [node.column_name for node in results]
        lower_cols = [name.lower() for name in column_names]
        assert any("customer" in c for c in lower_cols)

    def test_column_wildcard_contains_with_namespace_prefix(self):
        """Test column wildcard contains semantics with namespace prefix."""
        results = self.graph.find_columns_wildcard("mssql://localhost/InfoTrackerDW..customer")
        assert len(results) >= 1
        
        # Extract column names from all results
        column_names = [node.column_name for node in results]
        lower_cols = [name.lower() for name in column_names]
        assert any("customer" in c for c in lower_cols)

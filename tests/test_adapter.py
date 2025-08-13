"""
Integration tests for the MssqlAdapter component.
"""
import pytest
import json

from infotracker.adapters import MssqlAdapter
from .conftest import assert_json_equal


class TestMssqlAdapter:
    """Test cases for MssqlAdapter functionality."""

    def setup_method(self):
        """Set up test instance."""
        self.adapter = MssqlAdapter()

    def test_extract_lineage_customers_table(self, sql_content, expected_lineage):
        """Test lineage extraction for customers table."""
        sql = sql_content["01_customers"]
        result_json = self.adapter.extract_lineage(sql, "01_customers")
        result = json.loads(result_json)
        expected = expected_lineage["01_customers"]
        
        # Compare key fields
        assert result["eventType"] == expected["eventType"]
        assert result["run"]["runId"] == expected["run"]["runId"]
        assert result["job"]["name"] == expected["job"]["name"]
        assert result["inputs"] == expected["inputs"]
        
        # Check output structure
        assert len(result["outputs"]) == 1
        output = result["outputs"][0]
        expected_output = expected["outputs"][0]
        
        assert output["namespace"] == expected_output["namespace"]
        assert output["name"] == expected_output["name"]
        
        # Check schema facet
        assert "schema" in output["facets"]
        schema_facet = output["facets"]["schema"]
        expected_schema = expected_output["facets"]["schema"]
        
        assert len(schema_facet["fields"]) == len(expected_schema["fields"])
        
        # Compare each field
        for actual_field, expected_field in zip(schema_facet["fields"], expected_schema["fields"]):
            assert actual_field["name"] == expected_field["name"]
            assert actual_field["type"] == expected_field["type"]

    def test_extract_lineage_stg_orders_view(self, sql_content, expected_lineage):
        """Test lineage extraction for stg_orders view."""
        sql = sql_content["10_stg_orders"]
        result_json = self.adapter.extract_lineage(sql, "10_stg_orders")
        result = json.loads(result_json)
        expected = expected_lineage["10_stg_orders"]
        
        # Compare key fields
        assert result["eventType"] == expected["eventType"]
        assert result["run"]["runId"] == expected["run"]["runId"]
        assert result["job"]["name"] == expected["job"]["name"]
        assert result["inputs"] == expected["inputs"]
        
        # Check output structure
        assert len(result["outputs"]) == 1
        output = result["outputs"][0]
        expected_output = expected["outputs"][0]
        
        assert output["namespace"] == expected_output["namespace"]
        assert output["name"] == expected_output["name"]
        
        # Check column lineage facet
        assert "columnLineage" in output["facets"]
        lineage_facet = output["facets"]["columnLineage"]
        expected_lineage_facet = expected_output["facets"]["columnLineage"]
        
        # Compare lineage fields
        assert len(lineage_facet["fields"]) == len(expected_lineage_facet["fields"])
        
        for field_name in expected_lineage_facet["fields"]:
            assert field_name in lineage_facet["fields"]
            actual_field = lineage_facet["fields"][field_name]
            expected_field = expected_lineage_facet["fields"][field_name]
            
            assert actual_field["transformationType"] == expected_field["transformationType"]
            assert len(actual_field["inputFields"]) == len(expected_field["inputFields"])
            
            # Compare input fields
            for actual_input, expected_input in zip(actual_field["inputFields"], expected_field["inputFields"]):
                assert actual_input["namespace"] == expected_input["namespace"]
                assert actual_input["name"] == expected_input["name"]
                assert actual_input["field"] == expected_input["field"]

    def test_error_handling(self):
        """Test error handling for invalid SQL."""
        invalid_sql = "INVALID SQL STATEMENT;"
        result_json = self.adapter.extract_lineage(invalid_sql, "test")
        result = json.loads(result_json)
        
        # Should return valid OpenLineage structure even on error
        assert "eventType" in result
        assert "run" in result
        assert "job" in result
        assert "outputs" in result

    @pytest.mark.parametrize("table_file", [
        "01_customers", "02_orders", "03_products", "04_order_items"
    ])
    def test_all_table_extractions(self, sql_content, expected_lineage, table_file):
        """Test lineage extraction for all table files."""
        if table_file not in sql_content or table_file not in expected_lineage:
            pytest.skip(f"Files for {table_file} not found")
            
        sql = sql_content[table_file]
        result_json = self.adapter.extract_lineage(sql, table_file)
        result = json.loads(result_json)
        expected = expected_lineage[table_file]
        
        # Basic structure validation
        assert result["eventType"] == "COMPLETE"
        assert result["run"]["runId"] == expected["run"]["runId"]
        assert result["inputs"] == []  # Tables have no inputs
        assert len(result["outputs"]) == 1
        
        # Schema facet should be present for tables
        output = result["outputs"][0]
        assert "schema" in output["facets"]

    @pytest.mark.parametrize("view_file", [
        "10_stg_orders", "11_stg_order_items", "12_stg_customers"
    ])
    def test_staging_view_extractions(self, sql_content, expected_lineage, view_file):
        """Test lineage extraction for staging view files."""
        if view_file not in sql_content or view_file not in expected_lineage:
            pytest.skip(f"Files for {view_file} not found")
            
        sql = sql_content[view_file]
        result_json = self.adapter.extract_lineage(sql, view_file)
        result = json.loads(result_json)
        expected = expected_lineage[view_file]
        
        # Basic structure validation
        assert result["eventType"] == "COMPLETE"
        assert result["run"]["runId"] == expected["run"]["runId"]
        assert len(result["inputs"]) > 0  # Views should have inputs
        assert len(result["outputs"]) == 1
        
        # Column lineage facet should be present for views
        output = result["outputs"][0]
        assert "columnLineage" in output["facets"]

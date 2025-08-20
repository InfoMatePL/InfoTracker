"""
Comprehensive tests comparing generated output with expected OpenLineage files.
"""
import pytest
import json
from pathlib import Path

from infotracker.adapters import MssqlAdapter
from .conftest import assert_json_equal, normalize_json


class TestExpectedOutputs:
    """Test cases comparing actual output with expected OpenLineage JSON files."""

    def setup_method(self):
        """Set up test instance."""
        self.adapter = MssqlAdapter()

    def test_customers_table_exact_match(self, sql_content, expected_lineage):
        """Test that customers table output exactly matches expected JSON."""
        sql = sql_content["01_customers"]
        result = json.loads(self.adapter.extract_lineage(sql, "01_customers"))
        expected = expected_lineage["01_customers"]
        
        # Normalize for comparison (handles formatting differences)
        result_norm = normalize_json(result)
        expected_norm = normalize_json(expected)
        
        # Compare structure components individually for better error messages
        assert result_norm["eventType"] == expected_norm["eventType"]
        assert result_norm["eventTime"] == expected_norm["eventTime"]
        assert result_norm["run"] == expected_norm["run"]
        assert result_norm["job"] == expected_norm["job"]
        assert result_norm["inputs"] == expected_norm["inputs"]
        
        # Compare outputs
        assert len(result_norm["outputs"]) == len(expected_norm["outputs"])
        result_output = result_norm["outputs"][0]
        expected_output = expected_norm["outputs"][0]
        
        assert result_output["namespace"] == expected_output["namespace"]
        assert result_output["name"] == expected_output["name"]
        
        # Compare schema facet
        assert "schema" in result_output["facets"]
        assert "schema" in expected_output["facets"]
        
        result_schema = result_output["facets"]["schema"]
        expected_schema = expected_output["facets"]["schema"]
        
        assert result_schema["_producer"] == expected_schema["_producer"]
        assert result_schema["_schemaURL"] == expected_schema["_schemaURL"]
        assert result_schema["fields"] == expected_schema["fields"]

    def test_stg_orders_view_exact_match(self, sql_content, expected_lineage):
        """Test that stg_orders view output exactly matches expected JSON."""
        sql = sql_content["10_stg_orders"]
        result = json.loads(self.adapter.extract_lineage(sql, "10_stg_orders"))
        expected = expected_lineage["10_stg_orders"]       
        # Normalize for comparison
        result_norm = normalize_json(result)
        expected_norm = normalize_json(expected)
        
        # Compare structure components
        assert result_norm["eventType"] == expected_norm["eventType"]
        assert result_norm["eventTime"] == expected_norm["eventTime"]
        assert result_norm["run"] == expected_norm["run"]
        assert result_norm["job"] == expected_norm["job"]
        assert result_norm["inputs"] == expected_norm["inputs"]
        
        # Compare outputs
        result_output = result_norm["outputs"][0]
        expected_output = expected_norm["outputs"][0]
        
        assert result_output["namespace"] == expected_output["namespace"]
        assert result_output["name"] == expected_output["name"]
        
        # Compare column lineage facet
        assert "columnLineage" in result_output["facets"]
        assert "columnLineage" in expected_output["facets"]
        
        result_lineage = result_output["facets"]["columnLineage"]
        expected_lineage_facet = expected_output["facets"]["columnLineage"]
        
        assert result_lineage["_producer"] == expected_lineage_facet["_producer"]
        assert result_lineage["_schemaURL"] == expected_lineage_facet["_schemaURL"]
        
        # Compare each lineage field
        result_fields = result_lineage["fields"]
        expected_fields = expected_lineage_facet["fields"]
        
        assert set(result_fields.keys()) == set(expected_fields.keys())
        
        for field_name in expected_fields:
            result_field = result_fields[field_name]
            expected_field = expected_fields[field_name]
            
            assert result_field["transformationType"] == expected_field["transformationType"]
            assert result_field["inputFields"] == expected_field["inputFields"]
            
            # Note: transformation descriptions might have minor formatting differences
            # We check that they contain similar content
            result_desc = result_field["transformationDescription"].replace(" ", "").lower()
            expected_desc = expected_field["transformationDescription"].replace(" ", "").lower()
            
            if field_name in ["OrderID", "CustomerID"]:
                # Identity transformations should be very similar
                assert field_name.lower() in result_desc
            elif field_name == "OrderDate":
                # CAST transformations
                assert "cast" in result_desc
                assert "date" in result_desc
            elif field_name == "IsFulfilled":
                # CASE transformations
                assert "case" in result_desc
                assert "status" in result_desc

    @pytest.mark.parametrize("file_stem", [
        "01_customers", "02_orders", "03_products", "04_order_items"
    ])
    def test_all_table_outputs(self, sql_content, expected_lineage, file_stem):
        """Test all table outputs match expected structure."""
        if file_stem not in sql_content or file_stem not in expected_lineage:
            pytest.skip(f"Files for {file_stem} not available")
        
        sql = sql_content[file_stem]
        result = json.loads(self.adapter.extract_lineage(sql, file_stem))
        expected = expected_lineage[file_stem]
        
        # Basic structure validation
        assert result["eventType"] == expected["eventType"]
        assert result["run"]["runId"] == expected["run"]["runId"]
        assert result["job"]["name"] == expected["job"]["name"]
        assert result["inputs"] == expected["inputs"]  # Empty for tables
        
        # Output validation
        assert len(result["outputs"]) == 1
        result_output = result["outputs"][0]
        expected_output = expected["outputs"][0]
        
        assert result_output["namespace"] == expected_output["namespace"]
        assert result_output["name"] == expected_output["name"]
        
        # Schema facet should be present and correct
        assert "schema" in result_output["facets"]
        result_schema = result_output["facets"]["schema"]
        expected_schema = expected_output["facets"]["schema"]
        
        assert len(result_schema["fields"]) == len(expected_schema["fields"])
        
        # Check that all expected fields are present with correct types
        result_field_map = {f["name"]: f["type"] for f in result_schema["fields"]}
        expected_field_map = {f["name"]: f["type"] for f in expected_schema["fields"]}
        
        assert result_field_map == expected_field_map

    @pytest.mark.parametrize("file_stem", [
        "10_stg_orders", "11_stg_order_items", "12_stg_customers"
    ])
    def test_staging_view_outputs(self, sql_content, expected_lineage, file_stem):
        """Test staging view outputs match expected structure."""
        if file_stem not in sql_content or file_stem not in expected_lineage:
            pytest.skip(f"Files for {file_stem} not available")
        
        sql = sql_content[file_stem]
        result = json.loads(self.adapter.extract_lineage(sql, file_stem))
        expected = expected_lineage[file_stem]
        
        # Basic structure validation
        assert result["eventType"] == expected["eventType"]
        assert result["run"]["runId"] == expected["run"]["runId"]
        assert result["job"]["name"] == expected["job"]["name"]
        assert result["inputs"] == expected["inputs"]  # Should have dependencies
        
        # Output validation
        assert len(result["outputs"]) == 1
        result_output = result["outputs"][0]
        expected_output = expected["outputs"][0]
        
        assert result_output["namespace"] == expected_output["namespace"]
        assert result_output["name"] == expected_output["name"]
        
        # Column lineage facet should be present
        assert "columnLineage" in result_output["facets"]
        result_lineage = result_output["facets"]["columnLineage"]
        expected_lineage_facet = expected_output["facets"]["columnLineage"]
        
        # Check that all expected columns have lineage
        result_columns = set(result_lineage["fields"].keys())
        expected_columns = set(expected_lineage_facet["fields"].keys())
        
        assert result_columns == expected_columns
        
        # Validate that each column has proper input fields
        for column_name in expected_columns:
            result_field = result_lineage["fields"][column_name]
            expected_field = expected_lineage_facet["fields"][column_name]
            
            assert result_field["transformationType"] == expected_field["transformationType"]
            assert len(result_field["inputFields"]) == len(expected_field["inputFields"])
            
            # Check input field structure
            for result_input, expected_input in zip(result_field["inputFields"], expected_field["inputFields"]):
                assert result_input["namespace"] == expected_input["namespace"]
                assert result_input["name"] == expected_input["name"]
                assert result_input["field"] == expected_input["field"]

    def test_full_pipeline_consistency(self, sql_content, expected_lineage):
        """Test that the full pipeline produces consistent results."""
        # Test that we can process all available files without errors
        processed_count = 0
        
        for file_stem in sql_content:
            if file_stem in expected_lineage:
                sql = sql_content[file_stem]
                result = json.loads(self.adapter.extract_lineage(sql, file_stem))

                # Basic validation for each file
                assert "eventType" in result
                assert "run" in result
                assert "job" in result
                assert "outputs" in result
                assert len(result["outputs"]) == 1
                
                processed_count += 1
        
        # Ensure we processed at least some files
        assert processed_count >= 4, f"Only processed {processed_count} files"

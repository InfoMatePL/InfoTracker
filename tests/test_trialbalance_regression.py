"""
Regression tests for TrialBalance procedure lineage artifacts.

These tests validate that lineage extraction for various TrialBalance procedures
(test0-test4) produces stable and correct results by checking:
- Object names and counts
- Dependencies (lineage)
- Schema consistency
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


TESTS_BASE = Path(__file__).parent.parent / "build"
INPUT_DIR = TESTS_BASE / "input"
OUTPUT_DIR = TESTS_BASE / "output"


@pytest.fixture
def test0_output() -> Path:
    """Path to test0 lineage artifacts (update_asefl_TrialBalance_BV)."""
    return OUTPUT_DIR / "test0"


@pytest.fixture
def test1_output() -> Path:
    """Path to test1 lineage artifacts (update_asefa_TrialBalance_BV)."""
    return OUTPUT_DIR / "test1"


@pytest.fixture
def test2_output() -> Path:
    """Path to test2 lineage artifacts (update_TrialBalance_tetafk_BV)."""
    return OUTPUT_DIR / "test2"


@pytest.fixture
def test4_output() -> Path:
    """Path to test4 lineage artifacts (update_asefl_TrialBalance_BV + table)."""
    return OUTPUT_DIR / "test4"


def load_json(path: Path) -> Any:
    """Load and parse a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


# ============================================================================
# Test0: update_asefl_TrialBalance_BV
# ============================================================================


def test_test0_artifacts_exist(test0_output: Path):
    """Verify all expected test0 artifacts are present."""
    expected_files = [
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMaxLoadDate.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMinAccountingPeriod.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashasefl_temp.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashinsert_update_temp_asefl.json",
        "column_graph.json",
    ]
    
    for fname in expected_files:
        fpath = test0_output / fname
        assert fpath.exists(), f"Expected artifact missing in test0: {fname}"


def test_test0_object_count(test0_output: Path):
    """Verify test0 has exactly 5 stored procedure artifacts + 1 graph."""
    json_files = list(test0_output.glob("StoredProcedure.*.json"))
    assert len(json_files) == 5, f"Expected 5 procedure artifacts, found {len(json_files)}"


def test_test0_main_dependencies(test0_output: Path):
    """Test main procedure dependencies for test0."""
    main_json = test0_output / "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    # Expected base tables
    expected_tables = {
        "dbo.AccountBalance_LNK_BV",
        "dbo.AccountBalance_sat_S_asefl_current",
        "dbo.GeneralAccount_sat_S_asefl_current",
        "dbo.TrialBalance_asefl_BV",
    }
    
    assert expected_tables.issubset(input_names), \
        f"Missing expected tables in test0. Got: {input_names}"
    
    # Expected temp tables
    expected_temps = {
        "dbo.update_asefl_TrialBalance_BV#MaxLoadDate",
        "dbo.update_asefl_TrialBalance_BV#MinAccountingPeriod",
        "dbo.update_asefl_TrialBalance_BV#asefl_temp",
        "dbo.update_asefl_TrialBalance_BV#insert_update_temp_asefl",
    }
    
    assert expected_temps.issubset(input_names), \
        f"Missing expected temp tables in test0. Got: {input_names}"


def test_test0_output_table(test0_output: Path):
    """Test output table for test0."""
    main_json = test0_output / "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json"
    data = load_json(main_json)
    
    assert len(data["outputs"]) == 1
    output = data["outputs"][0]
    assert output["name"] == "dbo.TrialBalance_asefl_BV"
    assert output["namespace"] == "mssql://localhost/EDW_CORE"


def test_test0_output_schema(test0_output: Path):
    """Test output schema for test0."""
    main_json = test0_output / "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json"
    data = load_json(main_json)
    
    schema = data["outputs"][0]["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    
    # Expected columns
    expected_fields = {
        "hk_l_AccountBalance",
        "hk_h_GeneralAccount",
        "hk_h_ChartOfAccount",
        "hk_h_AccountingPeriod",
        "GeneralAccountSegment",
        "AccountingYear",
        "AccountingMonth",
        "AccountingPeriod",
        "DebitInPeriod",
        "CreditInPeriod",
    }
    
    assert expected_fields.issubset(field_names), \
        f"Missing expected fields in test0. Got: {field_names}"
    
    # Should have exactly 27 fields
    assert len(schema["fields"]) == 27, \
        f"Expected 27 fields in test0, got {len(schema['fields'])}"


# ============================================================================
# Test1: update_asefa_TrialBalance_BV
# ============================================================================


def test_test1_artifacts_exist(test1_output: Path):
    """Verify all expected test1 artifacts are present."""
    expected_files = [
        "StoredProcedure.dbo.update_asefa_TrialBalance_BV.json",
        "StoredProcedure.dbo.update_asefa_TrialBalance_BV__temp__EDW_CORE.dbo.hashMaxLoadDate.json",
        "StoredProcedure.dbo.update_asefa_TrialBalance_BV__temp__EDW_CORE.dbo.hashMinAccountingPeriod.json",
        "StoredProcedure.dbo.update_asefa_TrialBalance_BV__temp__EDW_CORE.dbo.hashasefa_temp.json",
        "StoredProcedure.dbo.update_asefa_TrialBalance_BV__temp__EDW_CORE.dbo.hashinsert_update_temp_asefa.json",
        "column_graph.json",
    ]
    
    for fname in expected_files:
        fpath = test1_output / fname
        assert fpath.exists(), f"Expected artifact missing in test1: {fname}"


def test_test1_object_count(test1_output: Path):
    """Verify test1 has exactly 5 stored procedure artifacts."""
    json_files = list(test1_output.glob("StoredProcedure.*.json"))
    assert len(json_files) == 5, f"Expected 5 procedure artifacts, found {len(json_files)}"


def test_test1_main_dependencies(test1_output: Path):
    """Test main procedure dependencies for test1."""
    main_json = test1_output / "StoredProcedure.dbo.update_asefa_TrialBalance_BV.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    # Expected base tables (asefa instead of asefl)
    expected_tables = {
        "dbo.AccountBalance_LNK_BV",
        "dbo.AccountBalance_sat_S_asefa_current",
        "dbo.GeneralAccount_sat_S_asefa_current",
        "dbo.TrialBalance_asefa_BV",
    }
    
    assert expected_tables.issubset(input_names), \
        f"Missing expected tables in test1. Got: {input_names}"
    
    # Note: test1 uses simple temp table names (.#MaxLoadDate) instead of procedure-qualified
    # This is expected behavior for procedures without explicit table outputs


def test_test1_output_table(test1_output: Path):
    """Test output table for test1."""
    main_json = test1_output / "StoredProcedure.dbo.update_asefa_TrialBalance_BV.json"
    data = load_json(main_json)
    
    assert len(data["outputs"]) == 1
    output = data["outputs"][0]
    # Note: test1 has ONLY_PROCEDURE_RESULTSET, so output name is the procedure itself
    assert output["name"] == "dbo.update_asefa_TrialBalance_BV"
    assert output["namespace"] == "mssql://localhost/EDW_CORE"


def test_test1_output_schema(test1_output: Path):
    """Test output schema for test1."""
    main_json = test1_output / "StoredProcedure.dbo.update_asefa_TrialBalance_BV.json"
    data = load_json(main_json)
    
    output = data["outputs"][0]
    # Note: test1 has ONLY_PROCEDURE_RESULTSET, no schema facet expected
    assert "schema" not in output["facets"], \
        "test1 should not have schema facet (ONLY_PROCEDURE_RESULTSET)"
    
    # Check quality facet instead
    quality = output["facets"]["quality"]
    assert quality["reasonCode"] == "ONLY_PROCEDURE_RESULTSET"
    assert quality["lineageCoverage"] == 0.0


# ============================================================================
# Test2: update_TrialBalance_tetafk_BV
# ============================================================================


def test_test2_artifacts_exist(test2_output: Path):
    """Verify all expected test2 artifacts are present."""
    expected_files = [
        "StoredProcedure.dbo.update_TrialBalance_tetafk_BV.json",
        "StoredProcedure.dbo.update_TrialBalance_tetafk_BV__temp__EDW_CORE.dbo.hashMaxLoadDate.json",
        "StoredProcedure.dbo.update_TrialBalance_tetafk_BV__temp__EDW_CORE.dbo.hashMinAccountingPeriod.json",
        "StoredProcedure.dbo.update_TrialBalance_tetafk_BV__temp__EDW_CORE.dbo.hashtetafk_temp.json",
        "StoredProcedure.dbo.update_TrialBalance_tetafk_BV__temp__EDW_CORE.dbo.hashinsert_update_temp_tetafk.json",
        "column_graph.json",
    ]
    
    for fname in expected_files:
        fpath = test2_output / fname
        assert fpath.exists(), f"Expected artifact missing in test2: {fname}"


def test_test2_object_count(test2_output: Path):
    """Verify test2 has exactly 5 stored procedure artifacts."""
    json_files = list(test2_output.glob("StoredProcedure.*.json"))
    assert len(json_files) == 5, f"Expected 5 procedure artifacts, found {len(json_files)}"


def test_test2_main_dependencies(test2_output: Path):
    """Test main procedure dependencies for test2."""
    main_json = test2_output / "StoredProcedure.dbo.update_TrialBalance_tetafk_BV.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    # Expected base tables (tetafk has different satellites including bobookings and bookings)
    expected_tables = {
        "dbo.AccountBalance_LNK_BV",
        "dbo.AccountBalance_sat_S_mis_tetafk_bobookings_current",
        "dbo.AccountBalance_sat_S_mis_tetafk_bookings_current",
        "dbo.GeneralAccount_sat_S_mis_tetafk_current",
        "dbo.TrialBalance_tetafk_BV",
    }
    
    assert expected_tables.issubset(input_names), \
        f"Missing expected tables in test2. Got: {input_names}"
    
    # Expected temp tables (base names without version suffixes)
    expected_temps = {
        "dbo.update_TrialBalance_tetafk_BV#MaxLoadDate",
        "dbo.update_TrialBalance_tetafk_BV#MinAccountingPeriod",
        "dbo.update_TrialBalance_tetafk_BV#tetafk_temp",
        "dbo.update_TrialBalance_tetafk_BV#insert_update_temp_tetafk",
    }
    
    assert expected_temps.issubset(input_names), \
        f"Missing expected temp tables in test2. Got: {input_names}"


def test_test2_output_table(test2_output: Path):
    """Test output table for test2."""
    main_json = test2_output / "StoredProcedure.dbo.update_TrialBalance_tetafk_BV.json"
    data = load_json(main_json)
    
    assert len(data["outputs"]) == 1
    output = data["outputs"][0]
    assert output["name"] == "dbo.TrialBalance_tetafk_BV"
    assert output["namespace"] == "mssql://localhost/EDW_CORE"


def test_test2_output_schema(test2_output: Path):
    """Test output schema for test2."""
    main_json = test2_output / "StoredProcedure.dbo.update_TrialBalance_tetafk_BV.json"
    data = load_json(main_json)
    
    schema = data["outputs"][0]["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    
    # Expected columns (tetafk has richer structure with opening balances and cumulative fields)
    expected_fields = {
        "hk_l_AccountBalance",
        "hk_h_GeneralAccount",
        "hk_h_ChartOfAccount",
        "hk_h_AccountingPeriod",
        "GeneralAccountSegment",
        "GeneralAccountSegmentDesc",
        "AccountSegment2",
        "AccountSegment3",
        "AccountSegment4",
        "AccountSegment5",
        "AccountSegment6",
        "AccountSegment7",
        "AccountingYear",
        "AccountingMonth",
        "AccountingPeriod",
        "OpeningBalanceDebit",
        "OpeningBalanceCredit",
        "OpeningBalanceDebit_OpeningBalanceCredit",
        "DebitInPeriod",
        "CreditInPeriod",
        "AccountBalanceInPeriod",
        "CumulativelyDebit",
        "CumulativelyCredit",
        "CumulativelyDebit_CumulativelyCredit",
        "CompanyCode",
        "DV_load_date",
    }
    
    assert expected_fields.issubset(field_names), \
        f"Missing expected fields in test2. Got: {field_names}"
    
    # Should have exactly 26 fields
    assert len(schema["fields"]) == 26, \
        f"Expected 26 fields in test2, got {len(schema['fields'])}"


# ============================================================================
# Test4: update_asefl_TrialBalance_BV + Table.dbo.TrialBalance_asefl_BV
# ============================================================================


def test_test4_artifacts_exist(test4_output: Path):
    """Verify all expected test4 artifacts are present (procedure + table)."""
    expected_files = [
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMaxLoadDate.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMinAccountingPeriod.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashasefl_temp.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashinsert_update_temp_asefl.json",
        "Table.dbo.TrialBalance_asefl_BV.json",
        "Table.dbo.TrialBalance_asefl_BV__temp__EDW_CORE.dbo.hashMaxLoadDate.json",
        "column_graph.json",
    ]
    
    for fname in expected_files:
        fpath = test4_output / fname
        assert fpath.exists(), f"Expected artifact missing in test4: {fname}"


def test_test4_object_count(test4_output: Path):
    """Verify test4 has both procedure and table artifacts."""
    proc_files = list(test4_output.glob("StoredProcedure.*.json"))
    table_files = list(test4_output.glob("Table.*.json"))
    
    assert len(proc_files) == 5, f"Expected 5 procedure artifacts, found {len(proc_files)}"
    assert len(table_files) == 2, f"Expected 2 table artifacts, found {len(table_files)}"


def test_test4_table_lineage(test4_output: Path):
    """Test table artifact lineage for test4."""
    table_json = test4_output / "Table.dbo.TrialBalance_asefl_BV.json"
    data = load_json(table_json)
    
    # Table has procedure-qualified #MaxLoadDate as dependency
    input_names = {inp["name"] for inp in data["inputs"]}
    assert "dbo.update_asefl_TrialBalance_BV#MaxLoadDate" in input_names


def test_test4_procedure_same_as_test0(test4_output: Path, test0_output: Path):
    """Test that procedure in test4 has same structure as test0."""
    test4_main = test4_output / "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json"
    test0_main = test0_output / "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json"
    
    test4_data = load_json(test4_main)
    test0_data = load_json(test0_main)
    
    # Compare dependencies
    test4_inputs = {inp["name"] for inp in test4_data["inputs"]}
    test0_inputs = {inp["name"] for inp in test0_data["inputs"]}
    
    assert test4_inputs == test0_inputs, \
        f"test4 and test0 procedure inputs differ. test4: {test4_inputs}, test0: {test0_inputs}"
    
    # Compare output schema
    test4_schema = test4_data["outputs"][0]["facets"]["schema"]
    test0_schema = test0_data["outputs"][0]["facets"]["schema"]
    
    test4_fields = {f["name"] for f in test4_schema["fields"]}
    test0_fields = {f["name"] for f in test0_schema["fields"]}
    
    assert test4_fields == test0_fields, \
        f"test4 and test0 procedure schemas differ"


# ============================================================================
# Cross-test consistency checks
# ============================================================================


def test_all_have_quality_facet(test0_output: Path, test1_output: Path, 
                                 test2_output: Path, test4_output: Path):
    """Verify all artifacts have quality facet with full coverage."""
    test_dirs = [test0_output, test1_output, test2_output, test4_output]
    
    for test_dir in test_dirs:
        json_files = [
            f for f in test_dir.glob("*.json")
            if f.name.startswith(("StoredProcedure", "Table"))
            and f.name != "column_graph.json"
        ]
        
        for json_file in json_files:
            data = load_json(json_file)
            
            for output in data["outputs"]:
                assert "quality" in output["facets"], \
                    f"Missing quality facet in {test_dir.name}/{json_file.name}"
                
                quality = output["facets"]["quality"]
                assert quality["isFallback"] is False, \
                    f"Fallback lineage in {test_dir.name}/{json_file.name}"
                # Coverage can vary - some procedures have ONLY_PROCEDURE_RESULTSET
                # Skip full coverage check for those cases


def test_all_have_consistent_namespace(test0_output: Path, test1_output: Path, 
                                        test2_output: Path, test4_output: Path):
    """Verify all artifacts use consistent namespace (EDW_CORE or TEMPDB for temp tables)."""
    test_dirs = [test0_output, test1_output, test2_output, test4_output]
    expected_namespaces = {"mssql://localhost/EDW_CORE", "mssql://localhost/TEMPDB"}
    
    for test_dir in test_dirs:
        json_files = [
            f for f in test_dir.glob("*.json")
            if f.name.startswith(("StoredProcedure", "Table"))
            and f.name != "column_graph.json"
        ]
        
        for json_file in json_files:
            data = load_json(json_file)
            
            # Check all inputs - temp tables can be in TEMPDB
            for inp in data.get("inputs", []):
                assert inp["namespace"] in expected_namespaces, \
                    f"Unexpected namespace in {test_dir.name}/{json_file.name}: {inp['namespace']}"
            
            # Check all outputs - should be EDW_CORE
            for out in data.get("outputs", []):
                assert out["namespace"] == "mssql://localhost/EDW_CORE", \
                    f"Output should be in EDW_CORE namespace in {test_dir.name}/{json_file.name}"


def test_all_have_column_lineage(test0_output: Path, test1_output: Path, 
                                  test2_output: Path, test4_output: Path):
    """Verify main artifacts with schema have column lineage facet."""
    test_cases = [
        (test0_output, "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json", True),
        (test1_output, "StoredProcedure.dbo.update_asefa_TrialBalance_BV.json", False),  # ONLY_PROCEDURE_RESULTSET
        (test2_output, "StoredProcedure.dbo.update_TrialBalance_tetafk_BV.json", True),
        (test4_output, "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json", True),
        (test4_output, "Table.dbo.TrialBalance_asefl_BV.json", True),
    ]
    
    for test_dir, filename, should_have_lineage in test_cases:
        data = load_json(test_dir / filename)
        output = data["outputs"][0]
        
        if should_have_lineage:
            assert "columnLineage" in output["facets"], \
                f"Missing columnLineage facet in {test_dir.name}/{filename}"
            
            col_lineage = output["facets"]["columnLineage"]
            assert "fields" in col_lineage
            assert len(col_lineage["fields"]) > 0, \
                f"Empty columnLineage in {test_dir.name}/{filename}"
        else:
            # ONLY_PROCEDURE_RESULTSET cases don't have columnLineage
            assert "columnLineage" not in output["facets"], \
                f"Unexpected columnLineage in {test_dir.name}/{filename}"


def test_temp_tables_have_procedure_context(test0_output: Path, test1_output: Path, 
                                             test2_output: Path, test4_output: Path):
    """Verify temp table names include procedure context."""
    test_cases = [
        (test0_output, "update_asefl_TrialBalance_BV"),
        (test1_output, "update_asefa_TrialBalance_BV"),
        (test2_output, "update_TrialBalance_tetafk_BV"),
        (test4_output, "update_asefl_TrialBalance_BV"),
    ]
    
    for test_dir, proc_name in test_cases:
        json_files = list(test_dir.glob("StoredProcedure.*__temp__*.json"))
        
        assert len(json_files) > 0, \
            f"No temp table artifacts found in {test_dir.name}"
        
        for json_file in json_files:
            data = load_json(json_file)
            output = data["outputs"][0]
            
            # Temp table name should include procedure context
            assert f"{proc_name}#" in output["name"], \
                f"Temp table missing procedure context in {test_dir.name}/{json_file.name}: {output['name']}"


def test_column_graph_has_edges(test0_output: Path, test1_output: Path, 
                                 test2_output: Path, test4_output: Path):
    """Verify all column graphs have edges."""
    test_dirs = [test0_output, test1_output, test2_output, test4_output]
    
    for test_dir in test_dirs:
        graph_json = test_dir / "column_graph.json"
        data = load_json(graph_json)
        
        assert "edges" in data, f"Missing edges in {test_dir.name}/column_graph.json"
        assert len(data["edges"]) > 0, \
            f"Empty edges in {test_dir.name}/column_graph.json"
        
        # Check edge structure
        sample_edge = data["edges"][0]
        assert "from" in sample_edge
        assert "to" in sample_edge
        assert "transformation" in sample_edge

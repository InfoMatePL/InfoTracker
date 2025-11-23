"""
Regression test for debug_fix10 lineage artifacts.

This test validates that the lineage extraction for update_asefl_TrialBalance_BV
produces stable and correct results, including:
- Temp table naming with procedure context
- Column-level lineage through CTEs and window functions
- Final output to TrialBalance_asefl_BV
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def debug_fix10_dir() -> Path:
    """Path to the debug_fix10 lineage artifacts."""
    return Path("/home/pawel/projects/praktyki1/InfoTracker/build/output/debug_fix10")


def test_debug_fix10_artifacts_exist(debug_fix10_dir: Path):
    """Verify all expected lineage artifacts are present."""
    expected_files = [
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMaxLoadDate.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMinAccountingPeriod.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashasefl_temp.json",
        "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashinsert_update_temp_asefl.json",
        "column_graph.json",
    ]
    
    for fname in expected_files:
        fpath = debug_fix10_dir / fname
        assert fpath.exists(), f"Expected artifact missing: {fname}"


def test_main_procedure_output(debug_fix10_dir: Path):
    """Test the main procedure output to TrialBalance_asefl_BV."""
    main_json = debug_fix10_dir / "StoredProcedure.dbo.update_asefl_TrialBalance_BV.json"
    data = json.loads(main_json.read_text(encoding="utf-8"))
    
    # Check event metadata
    assert data["eventType"] == "COMPLETE"
    assert data["job"]["namespace"] == "infotracker/examples"
    
    # Check inputs include temp tables
    input_names = {inp["name"] for inp in data["inputs"]}
    assert "dbo.#asefl_temp" in input_names, "Missing #asefl_temp in inputs"
    assert "dbo.update_asefl_TrialBalance_BV#asefl_temp" in input_names, \
        "Missing procedure-context temp table in inputs"
    
    # Check output
    assert len(data["outputs"]) == 1
    output = data["outputs"][0]
    assert output["name"] == "dbo.TrialBalance_asefl_BV"
    assert output["namespace"] == "mssql://localhost/EDW_CORE"
    
    # Check schema facet
    schema = output["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    expected_fields = {
        "hk_l_AccountBalance",
        "hk_h_GeneralAccount",
        "hk_h_ChartOfAccount",
        "hk_h_AccountingPeriod",
        "GeneralAccountSegment",
        "AccountingYear",
        "AccountingMonth",
        "AccountingPeriod",
        "OpeningBalanceDebit",
        "OpeningBalanceCredit",
        "DebitInPeriod",
        "CreditInPeriod",
        "CumulativelyDebit",
        "CumulativelyCredit",
        "CompanyCode",
        "DV_load_date",
    }
    assert expected_fields.issubset(field_names), \
        f"Missing expected fields. Got: {field_names}"
    
    # Check column lineage facet
    col_lineage = output["facets"]["columnLineage"]
    assert "hk_l_AccountBalance" in col_lineage["fields"]
    assert "DebitInPeriod" in col_lineage["fields"]
    
    # Check quality facet
    quality = output["facets"]["quality"]
    assert quality["isFallback"] is False
    assert quality["lineageCoverage"] == 1.0


def test_asefl_temp_lineage(debug_fix10_dir: Path):
    """Test lineage for the #asefl_temp CTE."""
    temp_json = debug_fix10_dir / "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashasefl_temp.json"
    data = json.loads(temp_json.read_text(encoding="utf-8"))
    
    # Check inputs
    input_names = {inp["name"] for inp in data["inputs"]}
    expected_inputs = {
        "dbo.AccountBalance_LNK_BV",
        "dbo.AccountBalance_sat_S_asefl_current",
        "dbo.GeneralAccount_sat_S_asefl_current",
        "dbo.TrialBalance_asefl_BV",  # Self-join for historical data
    }
    assert expected_inputs.issubset(input_names), \
        f"Missing expected inputs. Got: {input_names}"
    
    # Check output has procedure context in name
    output = data["outputs"][0]
    assert output["name"] == "dbo.update_asefl_TrialBalance_BV#asefl_temp"
    assert output["namespace"] == "mssql://localhost/EDW_CORE"
    
    # Check schema
    schema = output["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    assert "DebitInPeriod" in field_names
    assert "CreditInPeriod" in field_names
    
    # Verify numeric type preserved
    numeric_fields = [f for f in schema["fields"] if f["type"] == "numeric"]
    numeric_names = {f["name"] for f in numeric_fields}
    assert "DebitInPeriod" in numeric_names
    assert "CreditInPeriod" in numeric_names
    
    # Check column lineage
    col_lineage = output["facets"]["columnLineage"]["fields"]
    
    # CompanyCode should map from DV_Tenant_ID
    company_code_lineage = col_lineage["CompanyCode"]
    input_fields = {
        (inp["name"], inp["field"])
        for inp in company_code_lineage["inputFields"]
    }
    assert ("dbo.AccountBalance_LNK_BV", "DV_Tenant_ID") in input_fields
    
    # DebitInPeriod should have complex lineage with LAG window function
    debit_lineage = col_lineage["DebitInPeriod"]
    debit_inputs = {inp["name"] for inp in debit_lineage["inputFields"]}
    assert "dbo.AccountBalance_sat_S_asefl_current" in debit_inputs
    assert "dbo.TrialBalance_asefl_BV" in debit_inputs  # LAG from history


def test_column_graph_structure(debug_fix10_dir: Path):
    """Test the column-level dependency graph."""
    graph_json = debug_fix10_dir / "column_graph.json"
    data = json.loads(graph_json.read_text(encoding="utf-8"))
    
    edges = data["edges"]
    assert len(edges) > 0, "Column graph has no edges"
    
    # Check edge structure
    sample_edge = edges[0]
    assert "from" in sample_edge
    assert "to" in sample_edge
    assert "transformation" in sample_edge
    assert "description" in sample_edge
    
    # Build lookup for easier testing
    edge_pairs = {(e["from"], e["to"]) for e in edges}
    
    # Test temp table -> final table flow
    # Check for edges from TrialBalance_asefl_BV -> temp -> back to TrialBalance_asefl_BV
    temp_sources = [e for e in edges if "#asefl_temp" in e["to"]]
    assert len(temp_sources) > 0, "No edges to #asefl_temp"
    
    temp_destinations = [e for e in edges if "#asefl_temp" in e["from"]]
    assert len(temp_destinations) > 0, "No edges from #asefl_temp"
    
    # Check for procedure-qualified temp table names
    proc_temp_edges = [
        e for e in edges
        if "update_asefl_TrialBalance_BV#" in e["from"]
        or "update_asefl_TrialBalance_BV#" in e["to"]
    ]
    assert len(proc_temp_edges) > 0, \
        "No edges with procedure-qualified temp table names"
    
    # Verify naming format (database.schema.procedure#temptable)
    for edge in proc_temp_edges:
        node = edge["from"] if "update_asefl_TrialBalance_BV#" in edge["from"] else edge["to"]
        # Should be: mssql://localhost/EDW_CORE.dbo.update_asefl_TrialBalance_BV#asefl_temp.column
        parts = node.split(".")
        assert len(parts) >= 4, f"Invalid temp table naming format: {node}"
        
        # Check no square brackets in temp table names
        assert "[" not in node, f"Temp table has brackets: {node}"
        assert "]" not in node, f"Temp table has brackets: {node}"


def test_transformation_types(debug_fix10_dir: Path):
    """Test that transformation types are correctly identified."""
    graph_json = debug_fix10_dir / "column_graph.json"
    data = json.loads(graph_json.read_text(encoding="utf-8"))
    
    transformations = {e["transformation"] for e in data["edges"]}
    
    # Should have both IDENTITY and EXPRESSION transformations
    assert "IDENTITY" in transformations, "No IDENTITY transformations found"
    assert "EXPRESSION" in transformations, "No EXPRESSION transformations found"
    
    # Find expression transformations
    expr_edges = [e for e in data["edges"] if e["transformation"] == "EXPRESSION"]
    assert len(expr_edges) > 0
    
    # Check for computed columns (subtraction operations)
    computed_edges = [
        e for e in expr_edges
        if "_" in e["to"].split(".")[-1]  # Column name has underscore (computed)
    ]
    assert len(computed_edges) > 0, "No computed column edges found"


def test_maxloaddate_temp_table(debug_fix10_dir: Path):
    """Test lineage for #MaxLoadDate temp table."""
    temp_json = debug_fix10_dir / "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMaxLoadDate.json"
    data = json.loads(temp_json.read_text(encoding="utf-8"))
    
    # Check it reads from TrialBalance_asefl_BV
    input_names = {inp["name"] for inp in data["inputs"]}
    assert "dbo.TrialBalance_asefl_BV" in input_names
    
    # Check output name
    output = data["outputs"][0]
    assert output["name"] == "dbo.update_asefl_TrialBalance_BV#MaxLoadDate"
    
    # Check schema has expected columns
    schema = output["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    expected = {"hk_h_generalaccount", "hk_h_chartofaccount", "DV_LoadDate", "DV_LoadDateMax"}
    assert expected.issubset(field_names)


def test_minaccountingperiod_temp_table(debug_fix10_dir: Path):
    """Test lineage for #MinAccountingPeriod temp table."""
    temp_json = debug_fix10_dir / "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashMinAccountingPeriod.json"
    data = json.loads(temp_json.read_text(encoding="utf-8"))
    
    # Check inputs
    input_names = {inp["name"] for inp in data["inputs"]}
    assert "dbo.AccountBalance_LNK_BV" in input_names
    
    # Check output name
    output = data["outputs"][0]
    assert output["name"] == "dbo.update_asefl_TrialBalance_BV#MinAccountingPeriod"
    
    # Check schema - note: PrevAccountingPeriodYear may not be in actual output
    schema = output["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    expected = {
        "hk_h_GeneralAccount",
        "hk_h_ChartOfAccount",
        "MinAccountingPeriod",
        "PrevAccountingPeriod",
    }
    assert expected.issubset(field_names)


def test_insert_update_temp_table(debug_fix10_dir: Path):
    """Test lineage for #insert_update_temp_asefl (OUTPUT clause target)."""
    temp_json = debug_fix10_dir / "StoredProcedure.dbo.update_asefl_TrialBalance_BV__temp__EDW_CORE.dbo.hashinsert_update_temp_asefl.json"
    data = json.loads(temp_json.read_text(encoding="utf-8"))
    
    # This temp table is target of OUTPUT clause
    output = data["outputs"][0]
    assert output["name"] == "dbo.update_asefl_TrialBalance_BV#insert_update_temp_asefl"
    
    # Check minimal schema (only two columns from OUTPUT)
    schema = output["facets"]["schema"]
    field_names = {f["name"] for f in schema["fields"]}
    assert "hk_h_AccountingPeriod" in field_names
    assert "GeneralAccountSegment" in field_names


def test_lineage_coverage(debug_fix10_dir: Path):
    """Verify all artifacts have complete lineage coverage."""
    json_files = [
        f for f in debug_fix10_dir.glob("StoredProcedure.*.json")
        if f.name != "column_graph.json"
    ]
    
    for json_file in json_files:
        data = json.loads(json_file.read_text(encoding="utf-8"))
        
        # Check each output has quality facet
        for output in data["outputs"]:
            assert "quality" in output["facets"], \
                f"Missing quality facet in {json_file.name}"
            
            quality = output["facets"]["quality"]
            assert "isFallback" in quality
            assert "lineageCoverage" in quality
            
            # All should be non-fallback with full coverage
            assert quality["isFallback"] is False, \
                f"Fallback lineage in {json_file.name}"
            assert quality["lineageCoverage"] == 1.0, \
                f"Incomplete lineage coverage in {json_file.name}"


def test_consistent_namespaces(debug_fix10_dir: Path):
    """Verify all artifacts use consistent namespaces."""
    json_files = [
        f for f in debug_fix10_dir.glob("StoredProcedure.*.json")
        if f.name != "column_graph.json"
    ]
    
    expected_namespace = "mssql://localhost/EDW_CORE"
    
    for json_file in json_files:
        data = json.loads(json_file.read_text(encoding="utf-8"))
        
        # Check all inputs
        for inp in data.get("inputs", []):
            assert inp["namespace"] == expected_namespace, \
                f"Inconsistent input namespace in {json_file.name}: {inp['namespace']}"
        
        # Check all outputs
        for out in data.get("outputs", []):
            assert out["namespace"] == expected_namespace, \
                f"Inconsistent output namespace in {json_file.name}: {out['namespace']}"


def test_column_graph_temp_table_format(debug_fix10_dir: Path):
    """Verify temp tables in column graph use correct naming."""
    graph_json = debug_fix10_dir / "column_graph.json"
    data = json.loads(graph_json.read_text(encoding="utf-8"))
    
    # Find all temp table references
    temp_nodes = set()
    for edge in data["edges"]:
        for node in [edge["from"], edge["to"]]:
            if "#" in node:
                # Extract table name (before column)
                table_part = ".".join(node.split(".")[:-1])
                temp_nodes.add(table_part)
    
    assert len(temp_nodes) > 0, "No temp tables found in column graph"
    
    for temp_table in temp_nodes:
        # Should be: mssql://localhost/EDW_CORE.dbo.procedure#tempname
        # or: mssql://localhost/EDW_CORE.dbo.#tempname
        assert ".dbo." in temp_table, f"Missing schema in temp table: {temp_table}"
        assert "#" in temp_table, f"Missing # in temp table: {temp_table}"
        
        # Extract just the table name part after dbo.
        table_name = temp_table.split(".dbo.")[-1]
        
        if "update_asefl_TrialBalance_BV#" in table_name:
            # Procedure-qualified temp table
            assert table_name.startswith("update_asefl_TrialBalance_BV#"), \
                f"Invalid procedure context: {table_name}"
        else:
            # Simple temp table
            assert table_name.startswith("#"), \
                f"Simple temp table should start with #: {table_name}"

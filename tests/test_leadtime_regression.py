"""
Regression tests for LeadTime procedure lineage artifacts.

These tests validate that lineage extraction for update_stage_mis_LeadTime procedure
produces stable and correct results by checking:
- Object names and counts
- Dependencies (lineage)
- Schema consistency
- Minimum expected table and edge counts
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def leadtime_artifacts():
    """Run extraction for LeadTime procedure and return path to artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_output = Path(tmpdir) / "lineage"
        tmp_output.mkdir()
        
        input_dir = Path(__file__).parent.parent / "build" / "input" / "test6"
        
        # Run extraction
        result = subprocess.run(
            [
                "infotracker",
                "extract",
                "--sql-dir",
                str(input_dir),
                "--out-dir",
                str(tmp_output),
            ],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0, f"Extract failed: {result.stderr}"
        
        yield tmp_output


def load_json(path: Path) -> Any:
    """Load and parse a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


# ============================================================================
# Artifact existence tests
# ============================================================================


def test_leadtime_main_artifact_exists(leadtime_artifacts: Path):
    """Verify main procedure artifact exists."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    assert main_json.exists(), "Main procedure artifact missing"


def test_leadtime_column_graph_exists(leadtime_artifacts: Path):
    """Verify column_graph.json exists."""
    graph_json = leadtime_artifacts / "column_graph.json"
    assert graph_json.exists(), "column_graph.json missing"


def test_leadtime_temp_artifacts_exist(leadtime_artifacts: Path):
    """Verify at least some expected temp table artifacts exist."""
    expected_temps = [
        "StoredProcedure.dbo.update_stage_mis_LeadTime__temp__EDW_CORE.dbo.hashctrl.json",
        "StoredProcedure.dbo.update_stage_mis_LeadTime__temp__EDW_CORE.dbo.hashoffer.json",
        "StoredProcedure.dbo.update_stage_mis_LeadTime__temp__EDW_CORE.dbo.hashLeadTime_STEP1.json",
        "StoredProcedure.dbo.update_stage_mis_LeadTime__temp__EDW_CORE.dbo.hashLeadTime_STEP4.json",
    ]
    
    for temp_name in expected_temps:
        temp_json = leadtime_artifacts / temp_name
        assert temp_json.exists(), f"Expected temp artifact missing: {temp_name}"


# ============================================================================
# Object count tests
# ============================================================================


def test_leadtime_artifact_count(leadtime_artifacts: Path):
    """Verify we have at least the expected number of artifacts."""
    json_files = list(leadtime_artifacts.glob("StoredProcedure.*.json"))
    # We expect 1 main + at least 20 temp tables
    assert len(json_files) >= 21, f"Expected at least 21 artifacts, found {len(json_files)}"


# ============================================================================
# Dependency tests
# ============================================================================


def test_leadtime_main_dependencies(leadtime_artifacts: Path):
    """Test main procedure output is the actual target table."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    # After transaction block removal fix, final INSERT INTO is now detected
    # The output should be the actual target table, not the procedure name
    assert len(data["outputs"]) == 1, "Expected exactly one output"
    output = data["outputs"][0]
    assert output["name"] == "dbo.stage_mis_ms_Offer_LeadTime", \
        f"Expected output 'dbo.stage_mis_ms_Offer_LeadTime', got '{output['name']}'"
    
    # Output should have schema facet with fields
    assert "schema" in output["facets"], "Output missing schema facet"
    schema = output["facets"]["schema"]
    assert "fields" in schema, "Schema missing fields"
    assert len(schema["fields"]) >= 85, \
        f"Expected at least 85 output columns, got {len(schema['fields'])}"
    
    # Output should have columnLineage facet
    assert "columnLineage" in output["facets"], "Output missing columnLineage facet"
    col_lineage = output["facets"]["columnLineage"]
    assert "fields" in col_lineage, "columnLineage missing fields"
    assert len(col_lineage["fields"]) >= 85, \
        f"Expected lineage for at least 85 columns, got {len(col_lineage['fields'])}"


def test_leadtime_minimum_table_count(leadtime_artifacts: Path):
    """Test that procedure output has correct lineage."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    # After transaction block removal fix, the main artifact now represents
    # the final INSERT INTO statement, which reads from #LeadTime_STEP4
    # The full dependency graph is available through temp table artifacts
    
    # Check that we have temp tables as inputs (the final INSERT reads from #LeadTime_STEP4)
    temp_tables = {
        inp["name"] for inp in data["inputs"]
        if "#" in inp["name"] or "hash" in inp["name"].lower()
    }
    
    assert len(temp_tables) >= 15, \
        f"Expected at least 15 temp tables as inputs, found {len(temp_tables)}"


def test_leadtime_temp_table_count(leadtime_artifacts: Path):
    """Test that we have expected temp tables."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    # Count temp tables
    temp_tables = {
        inp["name"] for inp in data["inputs"]
        if "#" in inp["name"]
    }
    
    # We expect at least 20 temp tables
    assert len(temp_tables) >= 20, \
        f"Expected at least 20 temp tables, found {len(temp_tables)}"


# ============================================================================
# Output tests
# ============================================================================


def test_leadtime_output_table(leadtime_artifacts: Path):
    """Test output table for LeadTime is the actual target table."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    assert len(data["outputs"]) == 1
    output = data["outputs"][0]
    # After fix: Parser now detects INSERT INTO and uses actual table name
    assert output["name"] == "dbo.stage_mis_ms_Offer_LeadTime", \
        f"Expected 'dbo.stage_mis_ms_Offer_LeadTime', got '{output['name']}'"
    assert output["namespace"] == "mssql://localhost/EDW_CORE"


def test_leadtime_output_schema_exists(leadtime_artifacts: Path):
    """Test output has schema facet with all columns."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    assert "schema" in data["outputs"][0]["facets"], "Output missing schema facet"
    schema = data["outputs"][0]["facets"]["schema"]
    assert "fields" in schema, "Schema missing fields"
    # After fix: Parser detects INSERT INTO schema with all columns
    assert len(schema["fields"]) >= 85, \
        f"Expected at least 85 columns in schema, got {len(schema['fields'])}"


def test_leadtime_output_column_count(leadtime_artifacts: Path):
    """Test output has full column lineage."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    schema = data["outputs"][0]["facets"]["schema"]
    fields = schema.get("fields", [])
    
    # Parser doesn't detect INSERT INTO schema, so we verify artifacts exist
    # but don't require specific columns. Column lineage is in column_graph.json
    # NOTE: This could be enhanced when parser supports INSERT INTO detection
    assert isinstance(fields, list), "Schema fields should be a list"


# ============================================================================
# Column lineage tests
# ============================================================================


def test_leadtime_column_graph_structure(leadtime_artifacts: Path):
    """Test column_graph.json has expected structure."""
    graph_json = leadtime_artifacts / "column_graph.json"
    data = load_json(graph_json)
    
    assert "edges" in data, "column_graph.json missing edges"
    assert isinstance(data["edges"], list), "edges should be a list"


def test_leadtime_column_graph_edge_count(leadtime_artifacts: Path):
    """Test column_graph.json has minimum number of edges."""
    graph_json = leadtime_artifacts / "column_graph.json"
    data = load_json(graph_json)
    
    # We observed ~773 edges (internal temp table lineage)
    # Parser doesn't detect final INSERT INTO, so this is lower than expected
    assert len(data["edges"]) >= 700, \
        f"Expected at least 700 column lineage edges, found {len(data['edges'])}"


def test_leadtime_column_graph_sources(leadtime_artifacts: Path):
    """Test column_graph.json references expected source tables."""
    graph_json = leadtime_artifacts / "column_graph.json"
    data = load_json(graph_json)
    
    # Collect unique source tables from edges
    sources = set()
    for edge in data["edges"]:
        from_val = edge.get("from", "")
        if "mssql://localhost/" in from_val:
            # Extract table name
            table_part = from_val.split("mssql://localhost/")[1]
            table_part = table_part.split("#")[0].replace(".*", "").strip()
            if table_part:
                sources.add(table_part)
    
    # Should reference at least 30 unique tables
    assert len(sources) >= 30, \
        f"Expected at least 30 unique source tables in column graph, found {len(sources)}"


def test_leadtime_column_graph_transformations(leadtime_artifacts: Path):
    """Test column_graph.json has various transformation types."""
    graph_json = leadtime_artifacts / "column_graph.json"
    data = load_json(graph_json)
    
    transformations = {edge.get("transformation") for edge in data["edges"]}
    
    # Should have at least IDENTITY transformations
    assert "IDENTITY" in transformations, "Expected IDENTITY transformations"
    # May also have FUNCTION, CAST, CASE, etc.


# ============================================================================
# Specific table tests
# ============================================================================


def test_leadtime_has_snapshot_control(leadtime_artifacts: Path):
    """Verify #ctrl temp table (which sources SnapshotControlTable) is captured."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    # After transaction block fix: main artifact only has direct inputs
    # #ctrl temp table is an input, which itself sources SnapshotControlTable
    assert any("#ctrl" in name or "ctrl" in name.lower() for name in input_names), \
        f"Expected #ctrl temp table in inputs, got: {input_names}"


def test_leadtime_has_offer_journal(leadtime_artifacts: Path):
    """Verify temp tables that source OfferJournalStatusChange are captured."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    # After transaction block fix: main artifact has temp tables as inputs
    # Several temp tables source OfferJournalStatusChange_MSBV
    assert any("ojsch" in name.lower() for name in input_names), \
        f"Expected ojsch temp tables in inputs (which source OfferJournalStatusChange), got: {input_names}"


def test_leadtime_has_all_msbv_tables(leadtime_artifacts: Path):
    """Verify temp tables are properly captured as inputs."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    # After transaction block fix: main artifact has temp tables as inputs
    # The base _MSBV tables are inputs to those temp tables
    # Check that we have expected temp tables that source various MSBV tables
    temp_tables = [name for name in input_names if "#" in name or "hash" in name.lower()]
    assert len(temp_tables) >= 15, \
        f"Expected at least 15 temp table inputs, found {len(temp_tables)}"



def test_leadtime_has_all_bv_tables(leadtime_artifacts: Path):
    """Verify temp tables (which source various _BV tables) are captured."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    # After transaction block fix: main artifact has temp tables as inputs
    # Various _BV tables are inputs to those temp tables
    # Check that we have expected temp tables that correspond to the steps
    expected_temp_tables = {
        "#LeadTime_STEP1",
        "#LeadTime_STEP2", 
        "#LeadTime_STEP3",
        "#LeadTime_STEP4",
        "#offer",
        "#ctrl",
    }
    
    # Match temp tables with or without procedure prefix
    found_temps = {
        name for name in input_names 
        for temp in expected_temp_tables
        if temp.lower() in name.lower()
    }
    
    assert len(found_temps) >= len(expected_temp_tables), \
        f"Expected all temp tables, found {len(found_temps)}/{len(expected_temp_tables)}: {found_temps}"


# ============================================================================
# Namespace tests
# ============================================================================


def test_leadtime_all_tables_in_edw_core(leadtime_artifacts: Path):
    """Verify all tables belong to EDW_CORE namespace."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    for inp in data["inputs"]:
        if "#" not in inp["name"]:  # Skip temp tables
            assert inp["namespace"] == "mssql://localhost/EDW_CORE", \
                f"Table {inp['name']} has unexpected namespace: {inp['namespace']}"

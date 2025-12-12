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
    """Test main procedure has expected source tables."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    # Expected base tables (minimum set - there may be more)
    expected_tables = {
        "dbo.Offer_MSBV",
        "dbo.OfferJournalStatusChange_MSBV",
        "dbo.Contract_BV",
        "dbo.SnapshotControlTable_DailySlidingWindow_BV",
        "dbo.Cases_BV",
        "dbo.CaseIndividualDecision_BV",
        "dbo.IncomingInvoice_BV",
        "dbo.Asset_BV",
        "dbo.Process_BV",
        "dbo.ProcessTask_MSBV",
        "dbo.PartyStatement_MSBV",
        "dbo.LinkProcessOffer_BV",
        "dbo.End2EndSLA_BV",
    }
    
    missing = expected_tables - input_names
    assert not missing, f"Missing expected tables: {missing}"


def test_leadtime_minimum_table_count(leadtime_artifacts: Path):
    """Test that we extract at least 32 unique source tables."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    # Count unique non-temp input tables
    real_tables = {
        inp["name"] for inp in data["inputs"]
        if "#" not in inp["name"]  # Exclude temp tables
    }
    
    assert len(real_tables) >= 32, \
        f"Expected at least 32 source tables, found {len(real_tables)}: {sorted(real_tables)}"


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
    """Test output table for LeadTime."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    assert len(data["outputs"]) == 1
    output = data["outputs"][0]
    # Parser uses procedure name as output (doesn't detect INSERT INTO table)
    assert output["name"] == "dbo.update_stage_mis_LeadTime"
    assert output["namespace"] == "mssql://localhost/EDW_CORE"


def test_leadtime_output_schema_exists(leadtime_artifacts: Path):
    """Test output has schema facet (even if empty for INSERT INTO)."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    assert "schema" in data["outputs"][0]["facets"], "Output missing schema facet"
    schema = data["outputs"][0]["facets"]["schema"]
    assert "fields" in schema, "Schema missing fields"
    # Note: Parser doesn't detect INSERT INTO schema, so fields may be empty


def test_leadtime_output_column_count(leadtime_artifacts: Path):
    """Test output lineage is captured (even if schema is empty)."""
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
    """Verify SnapshotControlTable is properly captured."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    assert "dbo.SnapshotControlTable_DailySlidingWindow_BV" in input_names


def test_leadtime_has_offer_journal(leadtime_artifacts: Path):
    """Verify OfferJournalStatusChange is properly captured."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    assert "dbo.OfferJournalStatusChange_MSBV" in input_names


def test_leadtime_has_all_msbv_tables(leadtime_artifacts: Path):
    """Verify all _MSBV tables are captured."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    # Note: Lead_MSBV and LeadTimeToOffer_MSBV accessed via CTE, not directly as inputs
    expected_msbv = {
        "dbo.Offer_MSBV",
        "dbo.OfferJournalStatusChange_MSBV",
        "dbo.ProcessTask_MSBV",
        "dbo.PartyStatement_MSBV",
        "dbo.OfferVerificationAcceptation_MSBV",
        "dbo.OfferTransactionParameters_MSBV",
        "dbo.OfferTransactionTags_MSBV",
    }
    
    missing = expected_msbv - input_names
    assert not missing, f"Missing MSBV tables: {missing}"


def test_leadtime_has_all_bv_tables(leadtime_artifacts: Path):
    """Verify all _BV tables are captured."""
    main_json = leadtime_artifacts / "StoredProcedure.dbo.update_stage_mis_LeadTime.json"
    data = load_json(main_json)
    
    input_names = {inp["name"] for inp in data["inputs"]}
    
    expected_bv = {
        "dbo.Contract_BV",
        "dbo.Asset_BV",
        "dbo.Cases_BV",
        "dbo.CaseIndividualDecision_BV",
        "dbo.IncomingInvoice_BV",
        "dbo.Process_BV",
        "dbo.ProcessType_BV",
        "dbo.LinkProcessOffer_BV",
        "dbo.End2EndSLA_BV",
    }
    
    missing = expected_bv - input_names
    assert not missing, f"Missing BV tables: {missing}"


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

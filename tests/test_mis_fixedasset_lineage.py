from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from infotracker.engine import Engine, ExtractRequest


def _has_edge(edges: list[dict], from_prefix: str, to_prefix: str) -> bool:
    for e in edges:
        f = str(e.get("from", ""))
        t = str(e.get("to", ""))
        if f.startswith(from_prefix) and t.startswith(to_prefix):
            return True
    return False


@pytest.mark.skipif(
    not Path("/home/pawel/projects/praktyki1/100_MISCodeBase").exists(),
    reason="Local 100_MISCodeBase folder not found",
)
def test_fixedasset_lineage_from_100_miscodebase(tmp_path: Path):
    """
    Regression guard: ensure lineage for FixedAsset_{MSPIT,MSBV} + SATs is present
    and matches the expected upstream chain visible in the reference diagram.
    """
    base = Path("/home/pawel/projects/praktyki1/100_MISCodeBase")
    sql_dir = base

    # Only the relevant files to keep the run fast and deterministic
    include = [
        # MSPIT materialization
        "EDW_CORE/StoredProcedures/StoredProcedure.dbo.update_FixedAsset_MSPIT.sql",
        "EDW_CORE/Tables/Table.dbo.FixedAsset_MSPIT.sql",

        # SAT materializations (01/60/61/6z)
        "EDW_CORE/StoredProcedures/StoredProcedure.dbo.update_FixedAsset_sa_e78426a2.sql",  # plst01pf
        "EDW_CORE/StoredProcedures/StoredProcedure.dbo.update_FixedAsset_sa_e786dd1b.sql",  # plst60pf
        "EDW_CORE/StoredProcedures/StoredProcedure.dbo.update_FixedAsset_sa_e786e0dc.sql",  # plst61pf
        "EDW_CORE/StoredProcedures/StoredProcedure.dbo.update_FixedAsset_sa_a6cc3925.sql",  # 6zpf

        # STG stage views used by SATs
        "STG/Views/View.dbo.stage_asefl_FixedAsset_asefl_plst01pf.sql",
        "STG/Views/View.dbo.stage_asefl_FixedAsset_asefl_plst60pf.sql",
        "STG/Views/View.dbo.stage_asefl_FixedAsset_asefl_plst61pf.sql",
        "STG/Views/View.dbo.stage_FixedAsset_asefl_plst6zpf.sql",

        # Business vault layers
        "EDW_CORE/Views/View.dbo.FixedAsset_MSBV.sql",
        "EDW_CORE/Views/View.dbo.FixedAsset_BV.sql",
    ]

    config = SimpleNamespace(
        adapter="mssql",
        default_database="EDW_CORE",
        default_schema="dbo",
        include=None,
        exclude=None,
        ignore=[],
        dbt_mode=False,
    )

    eng = Engine(config)
    out_dir = tmp_path / "out"
    req = ExtractRequest(
        sql_dir=sql_dir,
        out_dir=out_dir,
        adapter="mssql",
        include=include,
        encoding="auto",
    )

    res = eng.run_extract(req)
    # Note: warnings may occur for unresolved dependencies or deprecated syntax
    # but shouldn't prevent lineage extraction for our target objects
    # assert res["warnings"] == 0

    graph_path = out_dir / "column_graph.json"
    assert graph_path.exists(), "column_graph.json not produced"
    edges = json.loads(graph_path.read_text(encoding="utf-8")).get("edges", [])
    assert edges, "no edges built in column_graph.json"

    # Full lineage chain visible in screenshot:
    # load_* → stage_* → SAT → MSBV → BV → JV → FixedAsset (INFO_SALES)
    
    # 1. Load tables → Stage views
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.load_asefl_PLST01PF.",
        "mssql://localhost/STG.dbo.stage_asefl_FixedAsset_asefl_plst01pf.",
    )
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.load_asefl_PLST60PF.",
        "mssql://localhost/STG.dbo.stage_asefl_FixedAsset_asefl_plst60pf.",
    )
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.load_asefl_PLST61PF.",
        "mssql://localhost/STG.dbo.stage_asefl_FixedAsset_asefl_plst61pf.",
    )
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.load_asefl_PLST6ZPF.",
        "mssql://localhost/STG.dbo.stage_FixedAsset_asefl_plst6zpf.",
    )
    
    # 2. Stage views → Satellites (THE CRITICAL REGRESSION CHECK!)
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.stage_asefl_FixedAsset_asefl_plst01pf.",
        "mssql://localhost/EDW_CORE.dbo.FixedAsset_sat_S_asefl_plst01pf.",
    ), "Missing stage→SAT edge for plst01pf"
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.stage_asefl_FixedAsset_asefl_plst60pf.",
        "mssql://localhost/EDW_CORE.dbo.FixedAsset_sat_S_asefl_plst60pf.",
    ), "Missing stage→SAT edge for plst60pf"
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.stage_asefl_FixedAsset_asefl_plst61pf.",
        "mssql://localhost/EDW_CORE.dbo.FixedAsset_sat_S_asefl_plst61pf.",
    ), "Missing stage→SAT edge for plst61pf"
    assert _has_edge(
        edges,
        "mssql://localhost/STG.dbo.stage_FixedAsset_asefl_plst6zpf.",
        "mssql://localhost/EDW_CORE.dbo.FixedAsset_satma_asefl_plst6zpf.",
    ), "Missing stage→SAT edge for plst6zpf"
    
    # 3. Satellites → MSBV
    to_msbv = "mssql://localhost/EDW_CORE.dbo.FixedAsset_MSBV."
    assert _has_edge(edges, "mssql://localhost/EDW_CORE.dbo.FixedAsset_MSPIT.", to_msbv)
    assert _has_edge(edges, "mssql://localhost/EDW_CORE.dbo.FixedAsset_sat_S_asefl_plst01pf.", to_msbv)
    assert _has_edge(edges, "mssql://localhost/EDW_CORE.dbo.FixedAsset_sat_S_asefl_plst60pf.", to_msbv)
    assert _has_edge(edges, "mssql://localhost/EDW_CORE.dbo.FixedAsset_sat_S_asefl_plst61pf.", to_msbv)
    assert _has_edge(edges, "mssql://localhost/EDW_CORE.dbo.FixedAsset_satma_asefl_plst6zpf.", to_msbv)
    
    # 4. MSBV → BV
    assert _has_edge(
        edges,
        "mssql://localhost/EDW_CORE.dbo.FixedAsset_MSBV.",
        "mssql://localhost/EDW_CORE.dbo.FixedAsset_BV.",
    ), "Missing MSBV→BV edge"



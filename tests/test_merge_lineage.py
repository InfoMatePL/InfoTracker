import json
from pathlib import Path

from infotracker.engine import Engine, ExtractRequest
from infotracker.config import load_config

BASE = Path(__file__).resolve().parent.parent
SQL_DIR = BASE / "examples" / "warehouse" / "sql2"
OUT_DIR = BASE / "build" / "test_merge"


def test_merge_produces_lineage_for_target(tmp_path):
    # Run extract on the single procedure + dependent view/table files
    cfg = load_config(None)
    engine = Engine(cfg)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    req = ExtractRequest(
        sql_dir=SQL_DIR,
        out_dir=OUT_DIR,
        adapter=cfg.default_adapter,
        include=["StoredProcedure.dbo.update_LeadPartner_sat_ms_mis.sql", "View.dbo.stage_mis_ms_LeadPartner.sql", "Table.dbo.LeadPartner_sat_ms_mis.sql"],
        exclude=[],
        fail_on_warn=False,
        encoding="auto",
    )
    result = engine.run_extract(req)
    assert result["warnings"] >= 0

    # MERGE lineage is emitted under the procedure artifact, with output dataset = target table
    sp_path = OUT_DIR / "StoredProcedure.dbo.update_LeadPartner_sat_ms_mis.json"
    assert sp_path.exists(), "Procedure artifact should be present"

    data = json.loads(sp_path.read_text(encoding="utf-8"))
    outs = data.get("outputs", [])
    assert outs, "outputs must not be empty"
    facets = outs[0].get("facets", {})
    col_lin = facets.get("columnLineage", {}).get("fields", {})

    # Expect DV_tenant_ID lineage field mapping from stage
    assert "DV_tenant_ID" in col_lin, "DV_tenant_ID must have lineage from MERGE USING source"

    # Stored procedure artifact should not include spurious dbo.SET input anymore
    inputs = data.get("inputs", [])
    assert all(inp.get("name") != "dbo.SET" for inp in inputs), "Should not include dbo.SET as dependency"

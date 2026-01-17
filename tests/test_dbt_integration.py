from __future__ import annotations

from pathlib import Path
import json

from infotracker.config import RuntimeConfig
from infotracker.engine import Engine, ExtractRequest


def test_dbt_job_name_and_namespace(tmp_path: Path):
    # Use example dbt models
    repo_root = Path(__file__).parent.parent
    models_dir = repo_root / "examples" / "dbt_warehouse" / "models"
    assert models_dir.exists(), "dbt example models directory is missing"

    out_dir = tmp_path / "out"

    cfg = RuntimeConfig()
    cfg.dbt_mode = True
    # Let dbt_project.yml provide defaults
    cfg.default_database = None
    cfg.default_schema = None

    engine = Engine(cfg)
    req = ExtractRequest(
        sql_dir=models_dir,
        out_dir=out_dir,
        adapter=cfg.default_adapter,
        include=["01_customers.sql", "10_stg_orders.sql", "02_orders.sql"],
        exclude=[],
        fail_on_warn=False,
        encoding="utf-8",
    )
    result = engine.run_extract(req)
    # Find produced file for 01_customers
    target = out_dir / "01_customers.json"
    assert target.exists(), f"Expected output JSON not found: {target}"
    payload = json.loads(target.read_text(encoding="utf-8"))
    # job name path for dbt
    assert payload["job"]["name"].endswith("dbt/models/01_customers.sql"), payload["job"]["name"]
    # output namespace should come from dbt_project.yml vars.default_database (DefaultDB)
    outputs = payload.get("outputs") or []
    assert outputs, "outputs should not be empty"
    assert outputs[0]["namespace"].endswith("/DefaultDB"), outputs[0]["namespace"]
    # output name should be default_schema.model_name (dbo.01_customers)
    assert outputs[0]["name"].startswith("dbo."), outputs[0]["name"]
    # schema facet should exist for schema-only seed/source models
    facets = outputs[0].get("facets", {})
    schema_f = facets.get("schema", {})
    assert schema_f and schema_f.get("fields"), "schema facet expected for dbt seed/source"

    # Also check a view model has columnLineage and mapped inputs from another model
    stg_target = out_dir / "10_stg_orders.json"
    assert stg_target.exists(), f"Expected output JSON not found: {stg_target}"
    stg_payload = json.loads(stg_target.read_text(encoding="utf-8"))
    stg_outputs = stg_payload.get("outputs") or []
    stg_facets = stg_outputs[0].get("facets", {}) if stg_outputs else {}
    col_ln = stg_facets.get("columnLineage", {})
    assert col_ln and col_ln.get("fields"), "columnLineage expected for stg_orders"
    # Find lineage for IsFulfilled -> should reference orders.Status
    fields = col_ln["fields"]
    if "IsFulfilled" in fields:
        inputs = fields["IsFulfilled"]["inputFields"]
        assert any(inp.get("name","" ).lower().endswith("dbo.orders") for inp in inputs)


def test_dbt_ephemeral_fallback(tmp_path: Path):
    # Create a minimal dbt project with a model that has no final SELECT
    proj = tmp_path / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "dbt_project.yml").write_text(
        """
name: 'tmp_proj'
version: '1.0.0'
config-version: 2
model-paths: ["models"]
vars:
  default_database: "MyDB"
  default_schema: "dbo"
        """.strip(),
        encoding="utf-8",
    )
    # Insert-only model (no top-level SELECT)
    (proj / "models" / "no_select.sql").write_text(
        """
-- dbt model without final SELECT
INSERT INTO #t SELECT 1;
        """.strip(),
        encoding="utf-8",
    )

    out_dir = tmp_path / "out"
    cfg = RuntimeConfig()
    cfg.dbt_mode = True
    engine = Engine(cfg)
    req = ExtractRequest(
        sql_dir=proj / "models",
        out_dir=out_dir,
        adapter=cfg.default_adapter,
        include=["no_select.sql"],
        exclude=[],
        fail_on_warn=False,
        encoding="utf-8",
    )
    engine.run_extract(req)
    target = out_dir / "no_select.json"
    assert target.exists(), "Expected fallback output JSON not found"
    payload = json.loads(target.read_text(encoding="utf-8"))
    # job path should be dbt/models/no_select.sql
    assert payload["job"]["name"].endswith("dbt/models/no_select.sql")
    # Output dataset name should be dbo.no_select (normalized)
    outputs = payload.get("outputs") or []
    assert outputs and outputs[0]["name"] == "dbo.no_select"
    # Fallback objects may have empty lineage facet but should exist
    # Namespace should reflect MyDB from dbt_project.yml
    assert outputs[0]["namespace"].endswith("mssql://localhost/MYDB")

### InfoTracker

This is a Python CLI that extracts column-level lineage from SQL, runs impact analysis, and detects breaking changes. First adapter targets MS SQL.

#### For Students
Start with a simple command: `infotracker extract --sql-dir examples/warehouse/sql/01_customers.sql --out-dir build/lineage`. This analyzes one SQL file.

#### Quickstart
```bash
pip install infotracker

# Extract lineage
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# Impact analysis
infotracker impact -s dbo.fct_sales.Revenue+

# Branch diff
infotracker diff --base main --head feature/x --sql-dir examples/warehouse/sql
```

#### Documentation
- `docs/overview.md` — what it is, goals, scope
- `docs/algorithm.md` — how extraction works
- `docs/lineage_concepts.md` — core concepts with visuals
- `docs/cli_usage.md` — commands and options
- `docs/breaking_changes.md` — definition and detection
- `docs/edge_cases.md` — SELECT *, UNION, temp tables, etc.
- `docs/adapters.md` — interface and MSSQL specifics
- `docs/architecture.md` — system and sequence diagrams
- `docs/configuration.md` — configuration reference
- `docs/openlineage_mapping.md` — how outputs map to OpenLineage
- `docs/faq.md` — common questions
- `docs/dbt_integration.md` — how to use with dbt projects

#### Requirements
- Python 3.10+
- Basic SQL
- Git and a shell

#### License
MIT (or your team’s preferred license) 
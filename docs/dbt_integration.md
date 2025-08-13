### Using InfoTracker with dbt

#### Why integrate InfoTracker with dbt?
InfoTracker analyzes SQL to produce column-level lineage, detect breaking changes, and run impact analysis. dbt organizes your SQL into models, tests, and macros. Combining them gives you:
- Early warning on breaking changes in models before merge
- Clear upstream/downstream impact when a column changes
- OpenLineage JSON you can publish or diff in CI

#### Prerequisites
- dbt project using SQL models
- For MS SQL: `dbt-sqlserver` or compatible adapter
- Python 3.10+, InfoTracker installed

```bash
pip install infotracker
```

### Recommended workflow

1) Compile dbt models
- dbt uses Jinja and macros. Compile first to get plain SQL.
```bash
dbt deps
dbt compile --target prod  # or your target
```
- Compiled SQL is under `target/compiled/<project>/models/`.

2) Run InfoTracker on compiled SQL
```bash
infotracker extract \
  --sql-dir target/compiled/<project>/models \
  --out-dir build/lineage
```

3) Compare to gold (optional but recommended)
- Keep expected lineage JSONs (gold) under version control, e.g., `examples/warehouse/lineage` or your project’s `gold/lineage`.
```bash
git diff --no-index gold/lineage build/lineage
```

4) Impact analysis during development
```bash
infotracker impact -s +dbo.my_model.OrderID+
infotracker impact -s my_db.my_schema.fact_orders.Revenue --direction upstream --max-depth 2
```

5) Breaking change detection in PRs
```bash
infotracker diff --base main --head $(git rev-parse --abbrev-ref HEAD) \
  --sql-dir target/compiled/<project>/models
```

### CI: GitHub Actions examples

Minimal warn-only PR check (does not fail CI):
```yaml
name: InfoTracker (dbt, warn-only)
on: [pull_request]
jobs:
  lineage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - name: Install deps
        run: |
          pip install infotracker dbt-core dbt-sqlserver  # use your dbt adapter
      - name: Compile dbt
        run: |
          dbt deps
          dbt compile --target prod
      - name: Detect breaking changes (warn-only)
        run: |
          set +e
          infotracker diff --base "${{ github.event.pull_request.base.ref }}" --head "${{ github.event.pull_request.head.ref }}" --sql-dir target/compiled/<project>/models
          EXIT=$?
          if [ "$EXIT" -eq 2 ]; then
            echo "::warning::Breaking changes detected (warn-only). Review the log."
          fi
          exit 0
```

Nightly regression (keeps the project healthy):
```yaml
name: Nightly Lineage Regression
on:
  schedule:
    - cron: '0 2 * * *'
jobs:
  regression:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - name: Install deps
        run: |
          pip install infotracker dbt-core dbt-sqlserver
      - name: Compile dbt
        run: |
          dbt deps
          dbt compile --target prod
      - name: Extract lineage
        run: |
          infotracker extract --sql-dir target/compiled/<project>/models --out-dir build/lineage
      - name: Compare to gold
        run: |
          git diff --no-index gold/lineage build/lineage || true
```

### Suggested configuration
Place an `infotracker.yml` at the repo root:
```yaml
default_adapter: mssql          # current adapter focus
sql_dir: target/compiled/<project>/models
out_dir: build/lineage
include: ["*.sql"]
exclude: ["**/tests/**", "**/analysis/**", "**/snapshots/**"]
severity_threshold: BREAKING
```
Tips:
- Use `exclude` to skip dbt tests/analysis directories
- If your dbt adapter changes relation naming, set `default_database`/`default_schema` in configuration to help qualification

### Model naming and selectors
- dbt models often become relations like `database.schema.model_name`
- Use InfoTracker selectors with fully qualified names when possible:
  - `mydb.my_schema.my_model.Column`
- If your environment uses different schemas per target, run InfoTracker in the same target to match names

### Benefits for dbt teams
- Catch breaking schema/semantic changes before merge
- Understand blast radius with upstream/downstream impact
- Keep stable, deterministic lineage artifacts for audits and reviews
- Use OpenLineage JSON to integrate with lineage platforms later

### Limitations and notes
- Run on compiled SQL to avoid Jinja/macros
- Initial adapter support is MS SQL; other engines can be added via adapters
- Dynamic SQL/macros that emit different shapes per run are out of scope for v1

### Next steps
- Add a small gold lineage set for your dbt project and wire CI diffs
- Start with a few critical models, then expand coverage
- See also: `docs/breaking_changes.md`, `docs/cli_usage.md`, `docs/lineage_concepts.md` 
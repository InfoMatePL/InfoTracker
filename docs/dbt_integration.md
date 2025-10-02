### Using InfoTracker with dbt

#### Why integrate InfoTracker with dbt?
InfoTracker analyzes SQL to produce column-level lineage, detect breaking changes, and run impact analysis. dbt organizes your SQL into models, tests, and macros. Combining them gives you:
- Early warning on breaking changes in models before merge
- Clear upstream/downstream impact when a column changes
- OpenLineage JSON you can publish or diff in CI

#### How dbt models differ from traditional SQL
dbt follows a unique paradigm that InfoTracker handles specially:

1. **SELECT-only models**: dbt models must end with a SELECT statement that defines the model output. Intermediate INSERT/UPDATE statements into #temp objects are allowed for complex transformations, but the final statement must be a pure SELECT.
2. **Materialization determined by config**: Whether a SELECT becomes a VIEW, TABLE (INSERT), or incremental UPDATE is defined in dbt_project.yml or model config, not in the SQL file
3. **Target object name from filename**: The output table/view name comes from the filename (e.g., `dim_customer.sql` → `dim_customer` table/view)
4. **Fully qualified source references**: dbt generates FROM clauses with fully qualified names (e.g., `SELECT attr FROM DBName.SchemaName.ObjectName`)
5. **Globally unique object names**: In dbt, object names are globally unique across the project. InfoTracker can ignore database and schema names from source references since the object name alone is sufficient to identify dependencies. All references are normalized to a default namespace (e.g., `DefaultDB.dbo.ObjectName`).

#### Prerequisites
- dbt project using SQL models
- For MS SQL: `dbt-sqlserver` or compatible adapter
- Python 3.10+, InfoTracker installed

```bash
pip install infotracker
```

#### dbt mode in InfoTracker
InfoTracker supports dbt models natively with the `--dbt` flag:

```bash
infotracker extract --dbt --sql-dir models/ --out-dir build/lineage
```

When `--dbt` mode is enabled:
- Parser expects models to end with a pure SELECT statement (intermediate INSERT/UPDATE to #temp objects are allowed)
- Target object name is derived from the SQL filename (without .sql extension)
- Database and schema names in FROM clauses are ignored and normalized to a default namespace, since dbt object names are globally unique
- Column lineage is tracked from sources through transformations to the final SELECT output

### Recommended workflow

#### 1) Compile dbt models
**IMPORTANT**: InfoTracker requires pure SQL and does NOT support Jinja templating. You must compile dbt models first.

dbt uses Jinja and macros. Always compile to get plain SQL:
```bash
dbt deps
dbt compile --target prod  # or your target
```
Compiled SQL is under `target/compiled/<project>/models/`.

#### 2) Run InfoTracker on compiled SQL
```bash
infotracker extract --dbt \
  --sql-dir target/compiled/<project>/models \
  --out-dir build/lineage
```

**Note**: Do not run InfoTracker on raw dbt model files containing `{{ ref() }}`, `{{ source() }}`, or other Jinja syntax. Only use compiled SQL output.

#### 3) Compare to gold (optional but recommended)
- Keep expected lineage JSONs (gold) under version control, e.g., `examples/warehouse/lineage` or your project’s `gold/lineage`.
```bash
git diff --no-index gold/lineage build/lineage
```

#### 4) Impact analysis during development
```bash
infotracker impact -s +dbo.my_model.OrderID+
infotracker impact -s my_db.my_schema.fact_orders.Revenue --direction upstream --max-depth 2
```

#### 5) Breaking change detection in PRs
```bash
# For dbt projects, use --dbt flag in diff as well
infotracker diff --dbt --base main --head $(git rev-parse --abbrev-ref HEAD) \
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
          infotracker diff --dbt --base "${{ github.event.pull_request.base.ref }}" --head "${{ github.event.pull_request.head.ref }}" --sql-dir target/compiled/<project>/models
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
          infotracker extract --dbt --sql-dir target/compiled/<project>/models --out-dir build/lineage
      - name: Compare to gold
        run: |
          git diff --no-index gold/lineage build/lineage || true
```

### Suggested configuration
Place an `infotracker.yml` at the repo root:
```yaml
default_adapter: mssql          # current adapter focus
dbt_mode: true                  # Enable dbt-specific parsing
sql_dir: target/compiled/<project>/models
out_dir: build/lineage
include: ["*.sql"]
exclude: ["**/tests/**", "**/analysis/**", "**/snapshots/**"]
severity_threshold: BREAKING
default_database: "analytics"   # Normalize database names in dbt models
default_schema: "dbo"           # Normalize schema names in dbt models
```
Tips:
- Set `dbt_mode: true` to enable dbt-specific parsing behavior
- Use `exclude` to skip dbt tests/analysis/snapshots directories
- Set `default_database` and `default_schema` to normalize all object references to a consistent namespace
- Since dbt object names are globally unique, database and schema names from SQL are ignored and replaced with these defaults (e.g., all objects become `analytics.dbo.object_name` regardless of what's in the FROM clause)

### Model naming and selectors
- In dbt mode, object names are globally unique and normalized to `default_database.default_schema.model_name`
- Use InfoTracker selectors with the normalized namespace:
  - `analytics.dbo.dim_customer.CustomerName` (if using default config)
  - Or just the object name if it's unambiguous: `dim_customer.CustomerName`
- Database and schema names from the original SQL are ignored during parsing

### Benefits for dbt teams
- Catch breaking schema/semantic changes before merge
- Understand blast radius with upstream/downstream impact
- Keep stable, deterministic lineage artifacts for audits and reviews
- Use OpenLineage JSON to integrate with lineage platforms later

### Limitations and notes
- **Jinja is NOT supported**: You MUST provide compiled SQL output from `dbt compile`. Raw dbt model files with Jinja templating will fail to parse.
- InfoTracker ignores database and schema names from SQL since dbt object names are globally unique. All objects are normalized to a default namespace.
- Models must end with a pure SELECT statement. Intermediate INSERT/UPDATE to #temp objects are allowed for complex transformations.
- Initial adapter support is MS SQL; other engines can be added via adapters
- Dynamic SQL/macros that emit different shapes per run are out of scope for v1

### Example dbt models
The repository includes example dbt-style models in `examples/dbt_warehouse/models/`:
- **staging models** (`stg_*.sql`): Clean and normalize source data
- **dimension models** (`dim_*.sql`): Build dimension tables from staging models
- **fact models** (`fct_*.sql`): Create fact tables with joins and aggregations
- **analytics views** (`vw_*.sql`): Complex analytics with CTEs and window functions

These examples demonstrate **compiled dbt SQL** (no Jinja):
- Models ending with pure SELECT statements
- Intermediate #temp table operations allowed (e.g., `90_usp_refresh_sales_with_temp.sql`)
- Fully qualified source references normalized to `DefaultDB.dbo.*`
- Database and schema names ignored since object names are globally unique
- Column transformations and lineage through staging → dimension → fact layers
- How InfoTracker tracks lineage from dbt model filename to output columns

Run the examples:
```bash
infotracker extract --dbt \
  --sql-dir examples/dbt_warehouse/models \
  --out-dir build/dbt_lineage
```

### Next steps
- Review the example dbt models in `examples/dbt_warehouse/models/`
- Add a small gold lineage set for your dbt project and wire CI diffs
- Start with a few critical models, then expand coverage
- See also: `docs/breaking_changes.md`, `docs/cli_usage.md`, `docs/lineage_concepts.md` 
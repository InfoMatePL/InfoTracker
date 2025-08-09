### CLI usage

#### Your spellbook (CLI incantations that actually work)
These commands are tried-and-true charms. Speak them clearly in your terminal, and columns will reveal their lineage.

- extract: “Show me the lineage!”
- impact: “Who depends on whom?”
- diff: “What changed, and will it cause problems?”

If a command fails, it’s not a curse—check flags and paths first.

#### Audience & prerequisites
- Audience: all users of the CLI (data/analytics/platform engineers)
- Prerequisites: basic SQL; Python 3.10+; shell and git

### Installation
```bash
pip install infotracker
infotracker --help
```

### Configuration file
- Optionally set defaults in `infotracker.yml`
```yaml
default_adapter: mssql
sql_dir: examples/warehouse/sql
out_dir: build/lineage
severity_threshold: BREAKING
include: ["*.sql"]
exclude: ["*_wip.sql"]
```
- Run with `--config infotracker.yml` (file may also live at repo root)

- Extract lineage
```
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage
```
- Impact analysis
```
infotracker impact -s dbo.vw_orders_final.OrderID    # default: downstream
infotracker impact -s +dbo.Orders.OrderID            # upstream from source (recursive)
infotracker impact -s dbo.stg_orders.OrderID+        # downstream from stg (recursive)
```
- Branch diff for breaking changes
```
infotracker diff --base main --head feature/x --sql-dir examples/warehouse/sql
``` 

### Global options
- `--config path.yml` load configuration
- `--log-level debug|info|warn|error`
- `--format json|text`

### Output formats
- Default output is text
- JSON: add `--format json` and redirect to a file
```bash
infotracker impact -s dbo.fct_sales.Revenue+ --format json > out.json
```

### extract
What Happens Step-by-Step:
1. Reads SQL files from the directory.
2. Analyzes them for lineage.
3. Writes JSON files to out-dir.

Usage:
```
infotracker extract --sql-dir DIR --out-dir DIR [--adapter mssql] [--catalog catalog.yml]
```
- Writes OpenLineage JSON per object into `out-dir`
- Options:
  - `--fail-on-warn` exit non-zero if warnings were emitted
  - `--include/--exclude` glob patterns for SQL files

Examples:
```bash
infotracker extract --sql-dir sql --out-dir build/lineage --include "stg_*.sql" --exclude "*_wip.sql"
infotracker extract --sql-dir sql --out-dir build/lineage --adapter mssql --catalog catalog.yml
```

### impact
Usage:
```
infotracker impact -s [+]schema.object.column[+] [--max-depth N] [--direction upstream|downstream] [--out out.json]
```
- Selector semantics: leading `+` = upstream seed; trailing `+` = downstream
- Output: list of columns with paths and reasons

Examples:
```bash
infotracker impact -s +dbo.Orders.OrderID+
infotracker impact -s dbo.fct_sales.Revenue --direction upstream --max-depth 2
```

### diff
Usage:
```
infotracker diff --base REF --head REF --sql-dir DIR [--adapter mssql] [--severity-threshold BREAKING]
```
- Compares base vs head, emits change list and impacts
- Exit codes: 0 no changes, 1 non-breaking only, 2 includes breaking (warn-only by default)

### Exit codes (quick)
- 0: nothing changed
- 1: changed but safe
- 2: breaking found (tool warns; your CI can still pass)

### Troubleshooting
- If no files found: check `--sql-dir` path. Example: If path is wrong, you'll see "No SQL files found in directory."
- Too many warnings: lower verbosity with `--log-level info`. Example: `infotracker extract --log-level info`
- JSON output needed: add `--format json` and redirect to a file
- Different results than expected: run on a single file first; check adapter/dialect flags

### Output JSON (impact, simplified)
```json
{
  "selector": "dbo.fct_sales.Revenue+",
  "direction": "downstream",
  "results": [
    {"object": "dbo.agg_sales_by_day", "column": "TotalRevenue", "path": ["dbo.fct_sales.Revenue", "dbo.agg_sales_by_day.TotalRevenue"], "reason": "AGGREGATION(SUM)"}
  ]
}
``` 

### See also
- `docs/overview.md`
- `docs/lineage_concepts.md`
- `docs/breaking_changes.md`
- `docs/configuration.md`
- `docs/architecture.md`
- `docs/openlineage_mapping.md`
- `docs/dbt_integration.md` 
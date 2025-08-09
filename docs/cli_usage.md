### CLI usage

#### Your spellbook (CLI incantations that actually work)
These commands are tried-and-true charms. Speak them clearly in your terminal, and columns will reveal their lineage.

- extract: “Show me the lineage!”
- impact: “Who depends on whom?”
- diff: “What changed, and will it explode?”

If a command fails, it’s not a curse—check flags and paths first.

- Extract lineage
```
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage
```
- Impact analysis
```
infotracker impact -s dbo.fct_sales.Revenue+     # downstream
infotracker impact -s +dbo.Orders.OrderStatus    # upstream
```
- Branch diff for breaking changes
```
infotracker diff --base main --head feature/x --sql-dir examples/warehouse/sql
``` 

### Global options
- `--config path.yml` load configuration
- `--log-level debug|info|warn|error`
- `--format json|text`

### extract
Usage:
```
infotracker extract --sql-dir DIR --out-dir DIR [--adapter mssql] [--catalog catalog.yml]
```
- Writes OpenLineage JSON per object into `out-dir`
- Options:
  - `--fail-on-warn` exit non-zero if warnings were emitted
  - `--include/--exclude` glob patterns for SQL files

### impact
Usage:
```
infotracker impact -s [+]schema.object.column[+] [--max-depth N] [--direction upstream|downstream] [--out out.json]
```
- Selector semantics: leading `+` = upstream seed; trailing `+` = downstream
- Output: list of columns with paths and reasons

### diff
Usage:
```
infotracker diff --base REF --head REF --sql-dir DIR [--adapter mssql] [--severity-threshold BREAKING]
```
- Compares base vs head, emits change list and impacts
- Exit codes: 0 no changes, 1 non-breaking only, 2 includes breaking

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
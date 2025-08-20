# InfoTracker Copilot Instructions

These rules guide the Copilot coding agent when working on this repo. Follow them strictly.

## Overview

InfoTracker is a T-SQL lineage and schema-diff tool with:

- **CLI**: extract, impact, diff
- **Core modules**: `parser.py`, `engine.py`, `diff.py`, `models.py`, `lineage.py`, `cli.py`
- **Outputs**: OpenLineage JSON artifacts

## Golden Rules (do/don't)

- **DO NOT** change public CLI surfaces or output formats.
- **DO** write small, focused commits with tests.
- **DO NOT** add one-off helper scripts. Run tasks via plain shell commands in CI/locally.
- **DO** keep logs tidy: WARNING for user-facing problems, DEBUG for details.
- **DO** keep behavior deterministic (stable sorting, IDs, output order).

## Parser (T-SQL)

### Preprocess before parsing

- Remove lines starting with `DECLARE`, `SET`, `PRINT`.
- Remove lone `GO` batch separators.
- Remove one-line `IF OBJECT_ID('tempdb..#…') … DROP TABLE #…` and bare `DROP TABLE #…` for temp tables.
- Join two-line `"INSERT INTO #tmp"` + next-line `"EXEC …"` into `"INSERT INTO #tmp EXEC …"`.

### Fallback when sqlglot.parse fails

- Detect `"INSERT INTO <table or #temp> EXEC <proc>"` (handle temp and regular tables).
- Clean proc name (no trailing semicolon, no parentheses).

### Build ObjectInfo with

- `name` = table or #temp
- `object_type` = table or temp_table
- `schema` = two placeholder columns (nullable)
- `dependencies` = {proc_name}
- `lineage` = ColumnReference(table_name=proc_name, column_name="*", transformation_type=EXEC)

### SELECT and CAST handling

- Obtain projections defensively: expressions, then projections, then args["expressions"].
- For `CAST(... AS TYPE)`, set `ColumnSchema.data_type` to the target type string (e.g., `DECIMAL(10,2)`).

### Namespace rules

- Temp tables under namespace `"tempdb"`.
- MSSQL datasets default to `"mssql://localhost/InfoTrackerDW"` unless config overrides.

## Engine (dependency graph)

- Build the graph from `ObjectInfo.dependencies` first. If empty, fallback to tables from `lineage.input_fields`.
- Keep topological sort independent of file order.
- On circular or missing dependencies: log WARNING and proceed (best-effort).

## OpenLineage

- Always add the schema facet for any object with known columns (tables, views, procedures/functions).
- Add the columnLineage facet when lineage exists.
- Keep namespace and dataset naming consistent.

## Diff

### Severities

- `COLUMN_TYPE_CHANGED` → `BREAKING`
- `COLUMN_RENAMED` → `POTENTIALLY_BREAKING`
- `COLUMN_ADDED` → `NON_BREAKING` if nullable, otherwise `POTENTIALLY_BREAKING`
- `OBJECT_ADDED` and `OBJECT_REMOVED` → `BREAKING`

Respect `severity_threshold` from config for table display and exit code.

## Wildcards (selectors)

- Empty or half-baked patterns (`"."`, `".."`, `"ns.."`) should return no results.

### Table wildcard `"schema.table.*"`

- Case-insensitive match by suffix segments (3, 2, or 1) of `table_name`; database prefix may differ.

### Column wildcard

- `"..pattern"` or `"prefix..pattern"`
- If pattern has no wildcard characters, treat as case-insensitive contains; otherwise use fnmatch.
- `"prefix"` with `"://"` filters by namespace; without `"://"` filters by `table_name` prefix.
- Deduplicate nodes before returning results.

## Tests and CI (no one-off scripts)

Add or adjust tests under `tests/` for every functional change.

### Unit tests

```bash
pytest -q
```

### Smoke (commands only; no helper scripts)

```bash
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

infotracker impact -s "+dbo.orders.total_amount+" --graph-dir build/lineage --max-depth 2 --format text
```

### Optional diff demo if folders exist

```bash
infotracker extract --sql-dir sql_diff_demo/base --out-dir build/ol_base || true

infotracker extract --sql-dir sql_diff_demo/head_non --out-dir build/ol_head || true

infotracker diff --base build/ol_base --head build/ol_head --format text || true
```

## Commit and PR standards

### Conventional commits

- `feat(parser|engine|diff|models|cli|lineage): …`
- `fix(parser|engine|…): …`
- `chore(logging|tests|ci): …`

### PR description

What changed, why, test coverage, and any impact on diff/impact/OpenLineage.

Keep PRs small and atomic. Do not mix refactor and feature unless mechanical.

## Definition of Done

- All tests pass (`pytest -q`).
- Smoke runs produce valid artifacts without new warnings (beyond known DECLARE/SET notices).
- Output formats unchanged (unless explicitly requested).
- `severity_threshold` respected in diff.

## Non-goals

- No new heavy dependencies.
- No changes to CLI names/flags or output JSON schemas.
- No suppressing errors by swallowing exceptions.

## If unsure

Ask in PR comments (mention maintainers), or leave a NOTE explaining the decision in code comments.
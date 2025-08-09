### Breaking changes: definition and detection

#### Guardians of the Data Contract (you are one now)
Your mission: protect downstream villagers (dashboards, jobs) from schema dragons and semantic shapeshifters. Spot trouble early; sleep better later.

- What breaks: names, types, nullability, meaning, order
- Why care: avoid 3 a.m. alerts and cursed KPIs
- How to win: run diffs in PRs; fix or document before merge

If the tool shouts “BREAKING,” that’s your bat-signal. Cape optional.

#### What is a breaking change?
A breaking change is any modification to a dataset’s external contract—its schema, semantics, or behavior—that can cause downstream code, jobs, or dashboards to fail or, worse, to silently produce incorrect results. This contract includes:
- Column presence and names (the interface)
- Data types, precision/scale, and nullability (the expectations)
- Expression semantics and aggregations (the meaning)
- Row-level properties such as keys, joins, and set alignment (the shape)

#### Why detect during the development cycle?
Catching breaking changes early prevents production incidents and aligns teams on a shared data contract.
- Reliability: avoids pipeline failures and broken dashboards post-deploy
- Safety: prevents silent data drift and misinterpretation by consumers
- Velocity: gives fast feedback in PRs so authors can fix before merge
- Governance: enforces data contracts and compliance requirements
- Cost: reduces expensive backfills, rollbacks, and incident time

Breaking changes include: removed/renamed columns, incompatible type/precision, nullability tightening, semantic expression/aggregation changes, join key/type changes, star expansion removals, ordinal mismatches for UNION/SELECT INTO, and object renames without updates.

Non-breaking (usually): adding unused columns, type widening.

See examples and detection algorithm:
- Parse base/head → build graphs → resolve schemas → diff schemas and expressions → classify severity → run impact analysis → report.

#### When to run detection in the dev cycle
- Local iteration: run against changed SQL before opening a PR
- Pre-commit or pre-push hook: warn early by default (do not block); optionally enforce later
- CI pull request check: compare branch vs base, surface warnings and a summary comment; do not fail by default
- Nightly on main: catch cross-repo/catalog drift and notify
- Release gating: optional; use only if your team agrees to enforcement

CLI:
```
infotracker diff --base main --head feature/x --sql-dir examples/warehouse/sql
```
Sample outputs include JSON change list and human-readable summary. 

### Severity levels
- BREAKING: will cause failures or misinterpretation downstream
- POTENTIALLY_BREAKING: requires review; may affect consumers (e.g., new column via `*`)
- NON_BREAKING: safe additions or widenings not exposed via `*`

### Compatibility matrix (illustrative)
- INT -> BIGINT: non-breaking
- DECIMAL(10,2) -> DECIMAL(12,2): non-breaking
- DECIMAL(18,2) -> INT: breaking (narrowing)
- VARCHAR(50) -> VARCHAR(100): non-breaking (widen)
- VARCHAR(100) -> VARCHAR(50): breaking (truncate risk)
- NULLABLE -> NOT NULL: potentially breaking unless source guarantees

### Rename detection heuristic
- If output expressions equal across versions but alias/name differs → classify as RENAME
- If column removed but a new column has identical lineage graph → suggest RENAME candidate

### Reporting
- Machine-readable JSON: list of changes with before/after schema, lineage deltas, severity, downstream impacts
- Human summary: one-line per change, grouped by object, with counts of impacted nodes
- Exit codes: 0 (no changes), 1 (changes but none breaking), 2 (breaking detected)

### Ignoring and baselining
- Allow `.infotrackerignore` patterns for objects/columns
- Support `--allow-widening` or `--ignore-additions` flags
- Baseline file to accept current state and compare future changes against it 

### Prerequisites and glossary
If you know basic SQL (SELECT, JOIN, GROUP BY), you have enough to start. These terms will appear often:
- **Schema**: the list of columns and their types for a table or view (its interface)
- **Column**: a named field like `OrderID` or `CustomerID`
- **Data type**: the kind of values a column stores (e.g., INT, VARCHAR(50), DECIMAL(10,2))
- **Precision/scale**: for decimals, total digits and digits after the decimal point
- **Nullability**: whether a column can contain NULL values
- **Aggregation**: functions like SUM/AVG with GROUP BY that combine multiple rows
- **Lineage**: how each output column is computed from input columns
- **Contract**: the promise a dataset makes to its consumers (names, types, meaning)

### Quick start: detect breaking changes locally
1. Make a branch and change some SQL under `examples/warehouse/sql` (or your SQL dir).
2. Run a diff against `main`:
   ```bash
   infotracker diff --base main --head $(git rev-parse --abbrev-ref HEAD) --sql-dir examples/warehouse/sql
   ```
3. Read the result:
   - Exit code `0`: no changes
   - Exit code `1`: changes but none breaking
   - Exit code `2`: breaking detected (warn by default; see actions below)

Example human-readable output:
```text
Object: dbo.fct_sales
- BREAKING: Column removed: TotalRevenue
- NON_BREAKING: Column added: TotalRevenue_v2 (expression identical)
```
Example JSON excerpt:
```json
{
  "object": "dbo.fct_sales",
  "changes": [
    { "type": "COLUMN_REMOVED", "name": "TotalRevenue", "severity": "BREAKING" },
    { "type": "COLUMN_ADDED", "name": "TotalRevenue_v2", "severity": "NON_BREAKING" }
  ]
}
```

### Hands-on examples (before → after → classification)
1) Rename a column
- Before:
  ```sql
  SELECT o.OrderID AS OrderID FROM dbo.Orders o;
  ```
- After:
  ```sql
  SELECT o.OrderID AS id FROM dbo.Orders o;
  ```
- Classification: BREAKING (downstream expecting `OrderID` will fail). If lineage/expression is identical, the tool may suggest RENAME.

2) Type widening
- Before: `UnitPrice DECIMAL(10,2)`  → After: `UnitPrice DECIMAL(12,2)`
- Classification: NON_BREAKING (more capacity, same meaning)

3) Nullability tightening
- Before: `CustomerEmail VARCHAR(200) NULL`  → After: `CustomerEmail VARCHAR(200) NOT NULL`
- Classification: POTENTIALLY_BREAKING (consumers may insert NULL or rely on NULL existing)

4) UNION column order mismatch
- Before:
  ```sql
  SELECT OrderID, CustomerID FROM dbo.Orders WHERE OrderStatus='shipped'
  UNION ALL
  SELECT OrderID, CustomerID FROM dbo.Orders WHERE OrderStatus='delivered';
  ```
- After (bug):
  ```sql
  SELECT CustomerID, OrderID FROM dbo.Orders WHERE OrderStatus='delivered';
  ```
- Classification: BREAKING (ordinal misalignment changes column meaning)

5) Adding an unused column
- Before: `SELECT OrderID FROM dbo.Orders` → After: `SELECT OrderID, CreatedAt FROM dbo.Orders`
- Classification: NON_BREAKING (unless consumers use `SELECT *` and rely on exact positions)

Tip: You can visualize column-level dependencies in `docs/lineage_concepts.md` to see why a change is breaking.

### Direct vs indirect lineage can break things
- Direct lineage change: you change the column itself (rename, remove, type change). Easy to see.
- Indirect lineage change: you change how rows are chosen or matched. This can also break downstream.

Examples:
- JOIN type change:
  ```sql
  -- Before
  SELECT o.OrderID, c.Region
  FROM dbo.Orders o LEFT JOIN dbo.Customers c ON o.CustomerID = c.CustomerID;
  -- After
  SELECT o.OrderID, c.Region
  FROM dbo.Orders o INNER JOIN dbo.Customers c ON o.CustomerID = c.CustomerID;
  ```
  After the change, orders without a matching customer disappear. Row count and nullability change. This is POTENTIALLY_BREAKING or BREAKING depending on consumers.

- Filter change:
  ```sql
  -- Before
  SELECT OrderID FROM dbo.Orders WHERE OrderStatus IN ('shipped','delivered');
  -- After
  SELECT OrderID FROM dbo.Orders WHERE OrderStatus = 'shipped';
  ```
  Fewer rows pass. Downstream sums or counts will change. This is a semantic change and can be BREAKING if users expect the old set.

- GROUP BY change:
  ```sql
  -- Before: by day
  SELECT CAST(OrderDate AS DATE) AS Day, SUM(Amount) AS Total FROM t GROUP BY CAST(OrderDate AS DATE);
  -- After: by month
  SELECT DATEFROMPARTS(YEAR(OrderDate),MONTH(OrderDate),1) AS Month, SUM(Amount) AS Total FROM t GROUP BY DATEFROMPARTS(YEAR(OrderDate),MONTH(OrderDate),1);
  ```
  The shape of data (granularity) changes. This is BREAKING for charts and joins that expect days.

Tip: even if output column names and types stay the same, join/filter/group changes can still break meaning. Treat them with care.

### Set up a simple pre-commit hook (warn-only)
Create `.git/hooks/pre-commit` (make it executable with `chmod +x .git/hooks/pre-commit`):
```bash
#!/usr/bin/env bash
set -euo pipefail
BASE_BRANCH="main"
HEAD_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
SQL_DIR="examples/warehouse/sql"

set +e
infotracker diff --base "$BASE_BRANCH" --head "$HEAD_BRANCH" --sql-dir "$SQL_DIR"
EXIT_CODE=$?
set -e

if [ "$EXIT_CODE" -eq 2 ]; then
  echo "[InfoTracker] Breaking changes detected (warn-only). Please review the report above."
  echo "Tip: Consider testing in a separate schema/DB and running regression before merging."
fi
# Always allow the commit
exit 0
```
This surfaces issues without blocking commits. You can switch to enforcement later if your team wants.

### CI example (GitHub Actions, warn-only)
Create `.github/workflows/infotracker-diff.yml`:
```yaml
name: InfoTracker Diff (Warn Only)
on: [pull_request]
jobs:
  diff:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - name: Install InfoTracker CLI
        run: pip install infotracker
      - name: Detect breaking changes (does not fail build)
        run: |
          set +e
          infotracker diff --base "${{ github.event.pull_request.base.ref }}" --head "${{ github.event.pull_request.head.ref }}" --sql-dir examples/warehouse/sql
          EXIT=$?
          if [ "$EXIT" -eq 2 ]; then
            echo "::warning::Breaking changes detected (warn-only). Review the log and plan rollout."
          fi
          exit 0
```
The job always passes but shows clear warnings if breaking changes are found.

### Communicate and act (suggested rollout)
- Communicate in the PR: list affected objects/columns and expected impact.
- Safe test: deploy all upstream objects to a separate schema/DB (e.g., `staging`), then:
  1) Point dependent views/jobs to `staging` temporarily
  2) Run `infotracker extract` on both old and new and compare outputs
  3) Run a small regression (sample queries, key metrics) to confirm results match your expectation
- If results look good, roll out to the main schema during a safe window.

### How to read the report (beginner-friendly)
- Start with the object name (e.g., `dbo.fct_sales`).
- Scan severities: fix BREAKING first, then review POTENTIALLY_BREAKING.
- For each change, note the column name, type/nullability changes, and any expression diffs.
- Use lineage hints to locate the exact input columns or transforms causing the change.

### Common pitfalls and how to avoid them
- Relying on `SELECT *`: prefer explicit column lists to avoid accidental contract changes.
- Silent type narrowing: be careful when changing DECIMAL precision/scale or casting to smaller types.
- UNION/SELECT INTO ordinals: keep column orders consistent and aligned.
- Implicit casts: make type conversions explicit so intent is clear to reviewers and tools.

### Reviewer checklist
- Are all breaking changes justified and communicated to consumers?
- Do renamed columns have matching lineage/expressions (true rename) vs. semantic change?
- Are type/precision/nullability changes intentional and documented?
- Are UNION/SELECT INTO column orders and types aligned?
- Do CI and pre-commit checks cover the changed SQL directories?

### Need more context?
- See `docs/lineage_concepts.md` for visual, column-level examples
- See `docs/cli_usage.md` for running the tool and command options
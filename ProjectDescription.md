### InfoTracker — Student Brief

#### Welcome to the Data Dungeon (friendly, no actual monsters)
You are the hero. Your quest: teach a tool to read SQL scrolls and tell true stories about columns. You’ll map where data comes from, spot traps (breaking changes), and keep the kingdom’s dashboards happy.

- Your gear: a CLI, example SQLs, and clear docs
- Your allies: adapters, lineage graphs, and CI checks
- Your enemies: sneaky `SELECT *`, UNION goblins, and 3 a.m. alerts
- Goal: green checks, clear diffs, and no broken charts

If you get stuck, it’s normal. Take a sip of tea, re-read the step, try a smaller example.

### For Beginners
If you're new to SQL, try this free tutorial: [Khan Academy SQL](https://www.khanacademy.org/computing/computer-programming/sql). It's in simple English and has exercises.

### Action plan (read and build in this order)
1) Understand the goal and scope (1 hour)
   - Overview: [docs/overview.md](docs/overview.md)
   - What you’re building, supported features, and what’s out of scope. Keep this open as your north star.

2) Learn column-level lineage basics (1-2 hours)
   - Concepts: [docs/lineage_concepts.md](docs/lineage_concepts.md)
   - Visual examples showing how each output column maps back to inputs (joins, transforms, aggregations). This informs how your extractor must reason.

3) Explore the example dataset (your training corpus)
   - Dataset map: [docs/example_dataset.md](docs/example_dataset.md)
   - Where the SQL files live, what each file represents (tables, views, CTEs, procs), and the matching OpenLineage JSON expectations you must reproduce.

4) Implement the algorithm incrementally
   - Algorithm: [docs/algorithm.md](docs/algorithm.md)
   - Steps: parse → object graph → schema resolution (expand `*` late) → column lineage extraction → impact graph → outputs.
   - Aim for correctness on simple files first, then progress to joins and aggregations.

5) Handle edge cases early enough to avoid rewrites
   - SELECT-star and star expansion: [docs/edge_cases.md](docs/edge_cases.md)
   - Requires object-level lineage first, then star expansion. Also watch UNION ordinals and SELECT INTO schema inference.

6) Decide on architecture and adapter boundaries
   - Adapters & extensibility: [docs/adapters.md](docs/adapters.md)
   - Define a clear adapter interface and implement MS SQL first (temp tables, variables, SELECT INTO, T-SQL functions). Keep the core engine adapter-agnostic.

7) Wire up the agentic workflow and regression tests
   - Agentic workflow: [docs/agentic_workflow.md](docs/agentic_workflow.md)
   - Loop the agent on the example corpus until the generated lineage matches the gold JSON. Add CI to auto-run on any SQL/lineage change.

8) Expose the CLI and iterate to parity
   - CLI usage: [docs/cli_usage.md](docs/cli_usage.md)
   - Implement `extract`, `impact`, and `diff`. The CLI is your acceptance surface; keep behavior stable and well-documented.

9) Implement breaking-change detection and reporting
   - Breaking changes: [docs/breaking_changes.md](docs/breaking_changes.md)
   - Compare base vs head branches: diff schemas/expressions, classify severity, compute downstream impacts, and emit machine + human-readable reports.

10) Optional: Integrate with dbt
   - dbt integration: [docs/dbt_integration.md](docs/dbt_integration.md)

### Milestones (suggested timebox)
- Day 1–2: read docs, install CLI, run extract on examples
- Day 3–5: implement simple lineage (no joins), pass gold files
- Day 6–8: add joins and aggregations, handle star expansion
- Day 9–10: wire warn-only diff in CI, polish docs

### Acceptance checklist
- Lineage matches gold JSONs in `examples/warehouse/lineage`
- Impact queries return correct columns for sample selectors
- Diff runs in CI (warn-only) and shows helpful messages
- Docs updated where needed; examples run without errors

### Quick-start CLI (target behavior)
Simple Example: To test one file, run `infotracker extract --sql-dir examples/warehouse/sql/01_customers.sql --out-dir build/lineage`

### Tips (pro-level, easy to follow)
- Start small: one view, then a join, then an aggregate
- Be explicit: avoid `SELECT *` while testing
- Commit often: small steps are easy to undo
- Use the example JSONs as your “gold” truth

### If stuck (quick help)
- Re-read the related doc step (linked above)
- Run the CLI with `--log-level debug` to see more info
- Create a tiny SQL with just the failing pattern and test that first
- Write down expected lineage for one column, then match it in code

### Agentic workflow and regression tests

#### Train your lineage familiar (it learns by fetching JSON)
Summon your agent, toss it SQL scrolls, and reward it when it returns with matching OpenLineage scrolls. Repeat until it purrs (tests pass).

- The loop: cast → compare → tweak → repeat
- The arena: `examples/warehouse/{sql,lineage}`
- Victory condition: exact matches, zero diffs, smug satisfaction

Remember: agents love clear acceptance criteria more than tuna.

- Prepare training set: SQL files + expected OpenLineage JSONs
- Loop (Cursor AI/CLI/web agents):
  1) Generate lineage → 2) Compare with expected → 3) Adjust prompts/code → 4) Repeat until pass
- CI: on any change under `examples/warehouse/{sql,lineage}`, run extraction and compare; fail on diffs
- Track coverage and edge cases (SELECT *, temp tables, UNION, variables) 

### Setup
- Install Cursor CLI and authenticate
- Organize repo with `examples/warehouse/{sql,lineage}` and a `build/` output folder

### Agent loop
1. Prompt template includes: adapter target (MS SQL), acceptance criteria (must match gold JSON), and allowed libraries (SQLGlot)
2. Agent writes code to `src/` and runs `infotracker extract` on the SQL corpus
3. Compare `build/lineage/*.json` to `examples/warehouse/lineage/*.json`
4. If diff exists, agent refines parsing/resolution rules and retries
5. Stop condition: all files match; record commit checkpoint

### CI integration
- GitHub Actions (example): on push/PR, run extraction and `git diff --no-index` against gold lineage; fail on differences
- Cache Python deps and AST caches for speed

### Evaluation metrics
- Exact-match rate across files
- Column coverage (percentage of outputs with lineage)
- Warning/error counts should trend down across iterations

### Updating gold files
- Intentional changes: regenerate lineage and review diffs; update gold JSON with PR describing the change 
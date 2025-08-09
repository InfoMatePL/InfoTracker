### Agentic workflow and regression tests

#### Train your lineage familiar (it learns by fetching JSON)
Summon your agent, toss it SQL scrolls, and reward it when it returns with matching OpenLineage scrolls. Repeat until it purrs (tests pass).

- The loop: cast → compare → tweak → repeat
- The arena: `examples/warehouse/{sql,lineage}`
- Victory condition: exact matches, zero diffs, tests pass (green)

Remember: agents love clear acceptance criteria more than tuna.

#### Audience & prerequisites
- Audience: engineers using agents/CIs to iterate on lineage extractors
- Prerequisites: basic SQL; Python; familiarity with CI and diff workflows

### Gold files (recap)
- Gold files = expected JSON lineage in `examples/warehouse/lineage`
- Your extractor must match them exactly (order and content)

### Fixing diffs (common)
- If a column mismatches: compare expressions; check alias qualification and star expansion timing
- If extra/missing columns: check join resolution and GROUP BY; ensure inputs are resolved before expansion
- If ordering differs: make outputs and diagnostics deterministic

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

### Loop diagram
```mermaid
flowchart LR
  G[Generate lineage] --> C[Compare to gold JSONs]
  C -->|diffs| R[Refine rules/prompts]
  R --> G
  C -->|no diffs| S[Stop (green)]
```

### Artifacts
- Inputs: SQL corpus under `examples/warehouse/sql`, optional catalog
- Outputs: `build/lineage/*.json`, diff logs, warnings
- CI artifacts: upload generated lineage for review

### Stop criteria
- All gold JSONs match exactly (order and content)
- No warnings if using `--fail-on-warn`

### CI integration
- GitHub Actions (example): on push/PR, run extraction and `git diff --no-index` against gold lineage; fail on differences
- Cache Python deps and AST caches for speed
- Upload generated `build/lineage/*.json` as CI artifacts for review

### Evaluation metrics
- Exact-match rate across files
- Column coverage (percentage of outputs with lineage)
- Warning/error counts should trend down across iterations

### Updating gold files
- Intentional changes: regenerate lineage and review diffs; update gold JSON with PR describing the change 

### See also
- `docs/example_dataset.md`
- `docs/algorithm.md`
- `docs/cli_usage.md`
- `docs/dbt_integration.md`

### Modus Operandi: Continuous Improvement with Cursor Agents
To extend the agentic workflow for 24/7/365 improvement of InfoTracker, integrate Cursor's web-based agents (like Background Agents) with GitHub. This builds on the regression testing and CI loops above, enabling automated code suggestions, bug fixes, and PR reviews. See [Cursor Changelog](https://cursor.com/changelog) for details.

#### Step 1: Set Up Cursor Web Agents for Continuous Improvement
1. **Enable Background Agents:** In Cursor, use Background Agents which run remotely and can be triggered via web (e.g., GitHub or Slack).
2. **Integrate with GitHub for 24/7 Operation:** Use GitHub Actions to schedule agent runs, combined with Cursor's API for AI tasks.
   - Create a workflow: `.github/workflows/cursor-improve.yml` to run daily.
   - Use Cursor's features like tagging @Cursor in issues for automated suggestions.
3. **Example Workflow for Scheduled Improvements:**
   ```yaml
   name: Cursor AI Improvement
   on: schedule
     - cron: '0 0 * * *' # Daily at midnight
   jobs:
     improve:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - name: Run Cursor Agent
           env:
             CURSOR_API_KEY: ${{ secrets.CURSOR_API_KEY }} # Add in GitHub Secrets
           run: |
             # Script to call Cursor API or simulate agent for code analysis
             python cursor_improve.py # Prompt agent to suggest repo improvements
   ```
4. **Script (cursor_improve.py):** Use Cursor's API to analyze code and open issues/PRs with suggestions.

#### Step 2: Set Up Cursor Agents for PR Reviews
1. **GitHub PR Integration:** In GitHub, tag @Cursor in PR comments to trigger Background Agent for reviews and fixes.
   - Example: In a PR, comment "@Cursor review this for bugs" – it will analyze and suggest changes.
2. **Automate with Workflow:** Trigger on PR events to auto-invoke Cursor.
   ```yaml
   name: Cursor PR Review
   on: pull_request
   jobs:
     review:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - name: Invoke Cursor Agent
           run: |
             # Use GitHub API to comment "@Cursor review" on the PR
             gh pr comment $PR_NUMBER --body "@Cursor review this PR for improvements"
   ```
3. **How It Works:** Cursor's Background Agent will read the PR, apply fixes if needed, and push commits (per changelog features).

#### Safeguards to Prevent Breaking Working Code
Constant improvements are great, but we must avoid breaking things that already work well. Here's how to build safety into the process:

- **Regression Testing:** Always run tests against gold standards (e.g., example JSONs in `examples/warehouse/lineage`). If an agent's suggestion changes outputs, reject it unless intentionally updating the gold.
- **CI/CD Pipelines:** Set up automated tests in GitHub Actions to run on every PR or scheduled run. Fail the build if tests break, catching issues early.
- **Human Oversight:** Agents suggest changes—review and approve them manually before merging. Use PR reviews to double-check.
- **Modular Changes:** Limit agent tasks to small, isolated improvements (e.g., one file at a time) to minimize risk.
- **Monitoring and Rollback:** Track metrics like test pass rates. Use Git for easy rollbacks if something breaks.

Integrate these into workflows: Add test steps to the example YAML files, and prompt agents with "Suggest improvements without changing existing correct behavior." 
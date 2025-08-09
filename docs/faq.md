### FAQ

#### Does InfoTracker execute SQL?
No. It parses and analyzes SQL to extract lineage; it does not run queries.

#### Is dynamic SQL supported?
Not in v1. Detect and warn; consider later instrumentation hooks.

#### How are user-defined functions handled?
- Scalar UDFs: treated as black-box functions over their input columns
- Table-valued functions: treated as source objects if schema is known

#### When is `*` expanded?
After upstream schemas are resolved in topological order.

#### Why do diffs change ordering sometimes?
Ensure deterministic ordering of outputs and diagnostics. Use the same adapter/config across runs.

#### How do I output JSON instead of text?
Add `--format json` and redirect to a file. 

#### Can I use InfoTracker with dbt?
Yes. Compile your dbt project (`dbt compile`) and point InfoTracker to `target/compiled/<project>/models`. See `docs/dbt_integration.md` for details. 
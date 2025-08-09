### High-level algorithm

#### Map the labyrinth (bring string, not MINUS)
To escape the SQL maze, you’ll build maps: object graphs, schemas, and column lineage webs. One step at a time, no Minotaur required.

- Scout terrain: parse files → objects → dependencies
- Place torches: resolve schemas, expand stars where safe
- Trace footprints: build column graph for impact and diff

If you see a star `*`, don’t panic—just expand it after you know what’s upstream.

1. Discover SQL assets and parse to AST (normalize identifiers)
2. Build object-level dependency graph (views, CTEs, procs/temp tables)
3. Resolve schemas topologically; expand `*` after inputs known
4. Extract column-level lineage per output column expression
5. Build bidirectional column graph for impact analysis
6. Detect breaking changes by diffing base vs head graphs/schemas/expressions
7. Output OpenLineage JSON + CLI reports 

### Data structures
- ObjectGraph: nodes = objects {name, type, statements}, edges = dependencies
- SchemaRegistry: map object -> [columns {name, type, nullable, ordinal}]
- ColumnGraph: nodes = fully qualified columns, edges = lineage relations

### Pseudocode (high-level)
```
files = load_sql(dir)
objects = parse(files)            # AST per object
objGraph = build_object_graph(objects)
order = topo_sort(objGraph)
for obj in order:
  schema_in = schemas_of_inputs(obj)
  schema_out, lineage = resolve(obj.AST, schema_in)
  SchemaRegistry[obj] = schema_out
  ColumnGraph.add(lineage)
```

### Resolve() essentials
- Name resolution: qualify identifiers using input schemas and aliases
- Star expansion: replace `*` with ordered columns from the resolved input
- Expression lineage: walk AST; collect input column refs per output column
- Type/nullable inference: derive from operations (e.g., CAST types, SUM numeric, CASE nullability = union of branches)
- Join semantics: track how join type affects nullability of columns
- Set ops: ensure column counts/types align; union lineage inputs

### Type/nullable rules (examples)
- `CAST(x AS T)` → type T
- `a + b` → numeric promotion; nullable if a or b nullable
- `CASE WHEN p THEN x ELSE y` → type = LUB(type(x), type(y)); nullable if either branch nullable or no ELSE
- `SUM(x)` → numeric; nullable unless GROUP present and engine semantics dictate otherwise

### Error handling and diagnostics
- On unresolved identifiers: record error with location; skip column lineage for affected outputs
- On unsupported syntax: emit warning; continue best-effort resolution
- Deterministic ordering of outputs and diagnostics for stable diffs

### Performance notes
- Cache parsed ASTs and resolved schemas by file hash
- Short-circuit lineage for unchanged objects between branches 
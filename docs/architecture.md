### Architecture

#### System diagram
```mermaid
graph LR
  U["User/CI"] --> C["CLI (infotracker)"]
  C --> E["Engine"]
  E --> A["Adapter (MSSQL)"]
  A --> P["Parser/Resolver (SQLGlot)"]
  P --> G["Graphs: Object / Schema / Column"]
  G --> O["OpenLineage JSON + Reports"]
```

#### Extract sequence
```mermaid
sequenceDiagram
  participant U as User/CI
  participant CLI as CLI
  participant ENG as Engine
  participant AD as Adapter
  participant RES as Parser/Resolver
  U->>CLI: infotracker extract
  CLI->>ENG: run
  ENG->>AD: select mssql
  AD->>RES: parse/resolve
  RES-->>ENG: schema + lineage
  ENG-->>CLI: OpenLineage JSON
```

#### Components
- CLI: argument parsing, command dispatch, IO
- Engine: orchestration, graph building, diffing
- Adapter: dialect-specific parsing, qualification, resolution
- Parser/Resolver: AST, schema inference, lineage extraction
- Outputs: OpenLineage payloads, text/JSON reports 
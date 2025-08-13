# InfoTracker AI Coding Agent Instructions

## Project Overview
InfoTracker is a Python CLI tool that extracts column-level SQL lineage, performs impact analysis, and detects breaking changes. It uses SQLGlot for parsing T-SQL/MS SQL and outputs OpenLineage-compliant JSON.

## Architecture & Key Components

### Core Flow (Extract → Graph → Analyze)
```
SQL Files → Adapter → Parser → Object Graph → Schema Resolution → Column Graph → OpenLineage JSON
```

**Essential files to understand:**
- `src/infotracker/cli.py` - Typer-based CLI with 3 main commands: extract, impact, diff
- `src/infotracker/engine.py` - Core orchestration with ExtractRequest/ImpactRequest/DiffRequest patterns
- `src/infotracker/models.py` - Data structures: ObjectInfo, ColumnLineage, ColumnGraph, TransformationType
- `src/infotracker/adapters.py` - Dialect-specific parsing (MssqlAdapter using SQLGlot)
- `src/infotracker/diff.py` - Breaking change detection with severity classification

### Data Models Pattern
- **ObjectInfo**: Contains schema + lineage for a SQL object (table/view)
- **ColumnLineage**: Maps output column to input columns with transformation type
- **ColumnGraph**: Bidirectional graph for impact analysis (upstream/downstream traversal)
- **TransformationType**: Enum for IDENTITY, CAST, AGGREGATE, EXPRESSION, etc.

### Adapter System
- Each adapter implements: `extract_lineage(sql: str) -> List[ObjectInfo]`
- MssqlAdapter uses SQLGlot with T-SQL dialect
- Returns OpenLineage JSON with columnLineage facets
- Error handling: continue processing other files if one fails

### Building Lineage Locally
```bash
# Extract all lineage
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# Test impact analysis
infotracker impact -s dbo.fct_sales.Revenue+  # downstream
infotracker impact -s +dbo.Orders.OrderID     # upstream

# Check breaking changes
infotracker diff --base main --head feature/x --sql-dir examples/warehouse/sql
```

### Working with Column Graph
- Call `_build_column_graph()` after extracting ObjectInfo list
- Use selectors: `+column` (upstream), `column+` (downstream), `+column+` (both)
- Graph has cycle detection and max-depth limiting

## Project-Specific Conventions

### SQL Processing Patterns
- **Topological resolution**: Parse object dependencies first, resolve schemas in dependency order
- **Star expansion**: Replace `SELECT *` after input schemas are known
- **Error isolation**: Bad files don't stop entire run - log and continue
- **Deterministic output**: Same input always produces same OpenLineage JSON

### T-SQL Specifics (MssqlAdapter)
- Case-insensitive identifiers, bracket quoting `[TableName]`
- Temp tables (`#temp`) handled specially
- Window functions parsed for column dependencies
- Common built-ins (GETDATE, DATEADD) treated as CONSTANT transformations

### Breaking Change Classification
- **BREAKING**: Column removed, type narrowed, object removed
- **POTENTIALLY_BREAKING**: Column added (affects SELECT *), order changed
- **NON_BREAKING**: Safe type widenings (INT→BIGINT)

### File Organization
- Examples in `examples/warehouse/sql/` with numbered prefixes (01_, 02_, etc.)
- Generated lineage in `build/lineage/` as individual JSON files
- Documentation in `docs/` with comprehensive coverage

## Integration Points

### OpenLineage Output Format
```json
{
  "job": {"name": "warehouse/sql/filename.sql"},
  "inputs": [{"namespace": "db", "name": "schema.table"}],
  "outputs": [{"facets": {"columnLineage": {"fields": {...}}}}]
}
```

### CLI Request/Response Pattern
- Commands use dataclass requests (ExtractRequest, ImpactRequest, DiffRequest)
- Engine methods return Dict[str, Any] for JSON serialization
- Output format controlled by `--format json|text`

### Configuration System
- `infotracker.yml` for project defaults
- CLI flags override config file values
- RuntimeConfig dataclass manages settings

## Common Debugging Approaches

### Lineage Gaps
- Verify object dependency order with `_build_object_graph()`
- Check schema resolution in topological order
- Star expansion happens after input schemas known

### Impact Analysis
- Build column graph with `_build_column_graph(objects)`
- Test selectors individually: upstream (`+col`), downstream (`col+`)
- Check for cycles in complex dependency chains

When implementing new features, follow the Request→Engine→Adapter→Models pattern and ensure OpenLineage output compatibility.

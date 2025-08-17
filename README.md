# InfoTracker

Column-level SQL lineage extraction and impact analysis for MS SQL Server

## Features

- **Column-level lineage** - Track data flow at the column level
- **Parse SQL files** and generate OpenLineage-compatible JSON
- **Impact analysis** - Find upstream and downstream column dependencies with flexible selectors
- **Wildcard matching** - Support for table wildcards (`schema.table.*`) and column wildcards (`..pattern`)
- **Direction control** - Query upstream (`+selector`), downstream (`selector+`), or both (`+selector+`)
- **Configurable depth** - Control traversal depth with `--max-depth`
- **Multiple output formats** - Text tables or JSON for scripting
- **MSSQL support** - T-SQL dialect with temp tables, variables, and stored procedures

## Requirements
- Python 3.10+
- Virtual environment (activated)
- Basic SQL knowledge
- Git and shell

## Troubleshooting
- **Error tracebacks on help commands**: Make sure you're running in an activated virtual environment
- **Command not found**: Activate your virtual environment first
- **Import errors**: Ensure all dependencies are installed with `pip install -e .`
- **Column not found**: Use full URI format or check column_graph.json for exact names

## Quickstart

### Setup & Installation
```bash
# Activate virtual environment first (REQUIRED)

# Install dependencies
pip install -e .

# Verify installation
infotracker --help
```

### Basic Usage
```bash
# 1. Extract lineage from SQL files (builds column graph)
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# 2. Run impact analysis
infotracker impact -s "STG.dbo.Orders.OrderID"  # downstream dependencies
infotracker impact -s "+STG.dbo.Orders.OrderID" # upstream sources
```

## Selector Syntax

InfoTracker supports flexible column selectors:

| Selector Format | Description | Example |
|-----------------|-------------|---------|
| `table.column` | Simple format (adds default `dbo` schema) | `Orders.OrderID` |
| `schema.table.column` | Schema-qualified format | `dbo.Orders.OrderID` |
| `database.schema.table.column` | Database-qualified format | `STG.dbo.Orders.OrderID` |
| `schema.table.*` | Table wildcard (all columns) | `dbo.fct_sales.*` |
| `..pattern` | Column wildcard (name contains pattern) | `..revenue` |
| `.pattern` | Alias for column wildcard | `.orderid` |
| Full URI | Complete namespace format | `mssql://localhost/InfoTrackerDW.STG.dbo.Orders.OrderID` |

### Direction Control
- `selector` - downstream dependencies (default)
- `+selector` - upstream sources  
- `selector+` - downstream dependencies (explicit)
- `+selector+` - both upstream and downstream

## Examples

```bash
# Extract lineage (run this first)
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# Find what feeds into a column (upstream)
infotracker impact -s "+dbo.fct_sales.Revenue"

# Find what uses a column (downstream) 
infotracker impact -s "STG.dbo.Orders.OrderID+"

# Find all relationships for columns containing "revenue"
infotracker impact -s "+..revenue+"

# Get all columns from a table
infotracker --format json impact -s "dbo.fct_sales.*"

# Limit traversal depth
infotracker impact -s "+dbo.Orders.OrderID" --max-depth 1
```

## Output Format

Impact analysis returns these columns:
- **from** - Source column (fully qualified)
- **to** - Target column (fully qualified)  
- **direction** - `upstream` or `downstream`
- **transformation** - Type of transformation (`IDENTITY`, `ARITHMETIC`, `UNION`, `WINDOW`, etc.)
- **description** - Human-readable transformation description

Results are automatically deduplicated. Use `--format json` for machine-readable output.

## Configuration

InfoTracker follows this configuration precedence:
1. **CLI flags** (highest priority) - override everything
2. **infotracker.yml** config file - project defaults  
3. **Built-in defaults** (lowest priority) - fallback values

Create an `infotracker.yml` file in your project root:
```yaml
default_adapter: mssql
sql_dir: examples/warehouse/sql
out_dir: build/lineage
include: ["*.sql"]
exclude: ["*_wip.sql"]
```

## Documentation

For detailed information:
- `docs/overview.md` — what it is, goals, scope
- `docs/algorithm.md` — how extraction works
- `docs/lineage_concepts.md` — core concepts with visuals
- `docs/cli_usage.md` — commands and options
- `docs/breaking_changes.md` — definition and detection
- `docs/edge_cases.md` — SELECT *, UNION, temp tables, etc.
- `docs/adapters.md` — interface and MSSQL specifics
- `docs/architecture.md` — system and sequence diagrams
- `docs/configuration.md` — configuration reference
- `docs/openlineage_mapping.md` — how outputs map to OpenLineage
- `docs/faq.md` — common questions

#### Documentation
- `docs/overview.md` — what it is, goals, scope
- `docs/algorithm.md` — how extraction works
- `docs/lineage_concepts.md` — core concepts with visuals
- `docs/cli_usage.md` — commands and options
- `docs/breaking_changes.md` — definition and detection
- `docs/edge_cases.md` — SELECT *, UNION, temp tables, etc.
- `docs/adapters.md` — interface and MSSQL specifics
- `docs/architecture.md` — system and sequence diagrams
- `docs/configuration.md` — configuration reference
- `docs/openlineage_mapping.md` — how outputs map to OpenLineage
- `docs/faq.md` — common questions
- `docs/dbt_integration.md` — how to use with dbt projects


## License
MIT (or your team’s preferred license) 
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
- **Advanced SQL objects** - Support for table-valued functions (TVF) and dataset-returning procedures
- **Temp table lineage** - Track EXEC into temp tables and propagate lineage downstream

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

### Selector Cheat Sheet

**Table wildcards:**
```bash
# All columns from a specific table
infotracker impact -s "dbo.fct_sales.*"
infotracker impact -s "STG.dbo.Orders.*"
```

**Column name matching:**
```bash
# Find all columns containing "revenue" (case-insensitive)
infotracker impact -s "..revenue"

# Find all columns containing "id" 
infotracker impact -s "..id"

# Use wildcards for pattern matching
infotracker impact -s "..customer*"
```

**Direction examples:**
```bash
# Upstream: what feeds into this column
infotracker impact -s "+dbo.fct_sales.Revenue"

# Downstream: what uses this column
infotracker impact -s "STG.dbo.Orders.OrderID+"

# Both directions
infotracker impact -s "+dbo.dim_customer.CustomerID+"
```

**Advanced SQL objects:**
```bash
# Table-valued function columns (upstream)
infotracker impact -s "+dbo.fn_customer_orders_tvf.*"

# Procedure dataset columns (upstream)  
infotracker impact -s "+dbo.usp_customer_metrics_dataset.*"

# Temp table lineage from EXEC
infotracker impact -s "+#temp_table.*"
```

## Examples

```bash
# Extract lineage (run this first)
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# Basic column lineage
infotracker impact -s "+dbo.fct_sales.Revenue"        # upstream sources
infotracker impact -s "STG.dbo.Orders.OrderID+"      # downstream usage

# Wildcard selectors
infotracker impact -s "+..revenue+"                   # all revenue columns (both directions)
infotracker impact -s "dbo.fct_sales.*"              # all columns from table
infotracker --format json impact -s "..customer*"     # customer columns (JSON output)

# Advanced SQL objects (NEW)
infotracker impact -s "+dbo.fn_customer_orders_tvf.*"      # TVF columns (upstream)
infotracker impact -s "+dbo.usp_customer_metrics_dataset.*" # procedure columns (upstream)

# Depth control
infotracker impact -s "+dbo.Orders.OrderID" --max-depth 1

# Demo the new features with the included examples
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage
infotracker impact -s "+dbo.fn_customer_orders_inline.*"
infotracker impact -s "+dbo.usp_customer_metrics_dataset.TotalRevenue"
```

### Copy-Paste Demo Commands

Test the new TVF and procedure lineage features:

```bash
# 1. Extract all lineage (including new TVF/procedure support)
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# 2. Test TVF lineage 
infotracker --format text impact -s "+dbo.fn_customer_orders_tvf.*"

# 3. Test procedure lineage
infotracker --format text impact -s "+dbo.usp_customer_metrics_dataset.*"

# 4. Test column name contains wildcard
infotracker --format text impact -s "+..revenue"

# 5. Show results in JSON format
infotracker --format json impact -s "..total*" > tvf_lineage.json
```

## Output Format

Impact analysis returns these columns:
- **from** - Source column (fully qualified)
- **to** - Target column (fully qualified)  
- **direction** - `upstream` or `downstream`
- **transformation** - Type of transformation (`IDENTITY`, `ARITHMETIC`, `AGGREGATION`, `CASE_AGGREGATION`, `DATE_FUNCTION`, `WINDOW`, etc.)
- **description** - Human-readable transformation description

Results are automatically deduplicated. Use `--format json` for machine-readable output.

### New Transformation Types

The enhanced transformation taxonomy includes:
- `ARITHMETIC_AGGREGATION` - Arithmetic operations combined with aggregation functions
- `COMPLEX_AGGREGATION` - Multi-step calculations involving multiple aggregations  
- `DATE_FUNCTION` - Date/time calculations like DATEDIFF, DATEADD
- `DATE_FUNCTION_AGGREGATION` - Date functions applied to aggregated results
- `CASE_AGGREGATION` - CASE statements applied to aggregated results

### Advanced Object Support

InfoTracker now supports advanced SQL Server objects:

**Table-Valued Functions (TVF):**
- Inline TVF (`RETURN AS SELECT`) - Parsed directly from SELECT statement
- Multi-statement TVF (`RETURN @table TABLE`) - Extracts schema from table variable definition
- Function parameters are tracked as filter metadata (don't create columns)

**Dataset-Returning Procedures:**
- Procedures ending with SELECT statement are treated as dataset sources
- Output schema extracted from the final SELECT statement  
- Parameters tracked as filter metadata affecting lineage scope

**EXEC into Temp Tables:**
- `INSERT INTO #temp EXEC procedure` patterns create edges from procedure columns to temp table columns
- Temp table lineage propagates downstream to final targets
- Supports complex workflow patterns combining functions, procedures, and temp tables

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
- `docs/advanced_use_cases.md` — tabular functions, procedures returning datasets
- `docs/adapters.md` — interface and MSSQL specifics
- `docs/architecture.md` — system and sequence diagrams
- `docs/configuration.md` — configuration reference
- `docs/openlineage_mapping.md` — how outputs map to OpenLineage
- `docs/faq.md` — common questions
- `docs/dbt_integration.md` — how to use with dbt projects


## License
MIT (or your team’s preferred license) 
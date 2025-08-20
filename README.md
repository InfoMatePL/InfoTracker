# InfoTracker

**Column-level SQL lineage extraction and impact analysis for MS SQL Server**

InfoTracker is a powerful command-line tool that parses T-SQL files and generates detailed column-level lineage in OpenLineage format. It supports advanced SQL Server features including table-valued functions, stored procedures, temp tables, and EXEC patterns.

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![PyPI](https://img.shields.io/badge/PyPI-InfoTracker-blue.svg)](https://pypi.org/project/InfoTracker/)

## 🚀 Features

- **Column-level lineage** - Track data flow at the column level with precise transformations
- **Advanced SQL support** - T-SQL dialect with temp tables, variables, CTEs, and window functions
- **Impact analysis** - Find upstream and downstream dependencies with flexible selectors
- **Wildcard matching** - Support for table wildcards (`schema.table.*`) and column wildcards (`..pattern`)
- **Breaking change detection** - Detect schema changes that could break downstream processes
- **Multiple output formats** - Text tables or JSON for integration with other tools
- **OpenLineage compatible** - Standard format for data lineage interoperability
- **Advanced SQL objects** - Table-valued functions (TVF) and dataset-returning procedures
- **Temp table tracking** - Full lineage through EXEC into temp tables

## 📦 Installation

### From PyPI (Recommended)
```bash
pip install InfoTracker
```

### From GitHub
```bash
# Latest stable release
pip install git+https://github.com/InfoMatePL/InfoTracker.git

# Development version
git clone https://github.com/InfoMatePL/InfoTracker.git
cd InfoTracker
pip install -e .
```

### Verify Installation
```bash
infotracker --help
```

## ⚡ Quick Start

### 1. Extract Lineage
```bash
# Extract lineage from SQL files
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage
```

### 2. Run Impact Analysis
```bash
# Find what feeds into a column (upstream)
infotracker impact -s "+STG.dbo.Orders.OrderID"

# Find what uses a column (downstream)  
infotracker impact -s "STG.dbo.Orders.OrderID+"

# Both directions
infotracker impact -s "+dbo.fct_sales.Revenue+"
```

### 3. Detect Breaking Changes
```bash
# Compare two versions of your schema
infotracker diff --base build/lineage --head build/lineage_new
```
## 📖 Selector Syntax

InfoTracker supports flexible column selectors for precise impact analysis:

| Selector Format | Description | Example |
|-----------------|-------------|---------|
| `table.column` | Simple format (adds default `dbo` schema) | `Orders.OrderID` |
| `schema.table.column` | Schema-qualified format | `dbo.Orders.OrderID` |
| `database.schema.table.column` | Database-qualified format | `STG.dbo.Orders.OrderID` |
| `schema.table.*` | Table wildcard (all columns) | `dbo.fct_sales.*` |
| `..pattern` | Column wildcard (name contains pattern) | `..revenue` |
| `..pattern*` | Column wildcard with fnmatch | `..customer*` |

### Direction Control
- `selector` - downstream dependencies (default)
- `+selector` - upstream sources  
- `selector+` - downstream dependencies (explicit)
- `+selector+` - both upstream and downstream

## 💡 Examples

### Basic Usage
```bash
# Extract lineage first (always run this before impact analysis)
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# Basic column lineage
infotracker impact -s "+dbo.fct_sales.Revenue"        # What feeds this column?
infotracker impact -s "STG.dbo.Orders.OrderID+"      # What uses this column?
```

### Wildcard Selectors
```bash
# All columns from a specific table
infotracker impact -s "dbo.fct_sales.*"
infotracker impact -s "STG.dbo.Orders.*"

# Find all columns containing "revenue" (case-insensitive)
infotracker impact -s "..revenue"

# Find all columns starting with "customer" 
infotracker impact -s "..customer*"
```

### Advanced SQL Objects
```bash
# Table-valued function columns (upstream)
infotracker impact -s "+dbo.fn_customer_orders_tvf.*"

# Procedure dataset columns (upstream)  
infotracker impact -s "+dbo.usp_customer_metrics_dataset.*"

# Temp table lineage from EXEC
infotracker impact -s "+#temp_table.*"
```

### Output Formats
```bash
# Text output (default, human-readable)
infotracker impact -s "+..revenue"

# JSON output (machine-readable)
infotracker --format json impact -s "..customer*" > customer_lineage.json

# Control traversal depth
infotracker impact -s "+dbo.Orders.OrderID" --max-depth 2
```

### Breaking Change Detection
```bash
# Extract baseline
infotracker extract --sql-dir sql_v1 --out-dir build/baseline

# Extract new version  
infotracker extract --sql-dir sql_v2 --out-dir build/current

# Detect breaking changes
infotracker diff --base build/baseline --head build/current

# Filter by severity
infotracker diff --base build/baseline --head build/current --threshold BREAKING
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

## 🔧 Configuration

Create an `infotracker.yml` file in your project root:

```yaml
sql_dirs:
  - "sql/"
  - "models/"
out_dir: "build/lineage"
exclude_dirs: 
  - "__pycache__"
  - ".git"
severity_threshold: "POTENTIALLY_BREAKING"
```

### Configuration Options

| Setting | Description | Default | Examples |
|---------|-------------|---------|----------|
| `sql_dirs` | Directories to scan for SQL files | `["."]` | `["sql/", "models/"]` |
| `out_dir` | Output directory for lineage files | `"lineage"` | `"build/artifacts"` |
| `exclude_dirs` | Directories to skip | `[]` | `["__pycache__", "node_modules"]` |
| `severity_threshold` | Breaking change detection level | `"NON_BREAKING"` | `"BREAKING"` |

## 📚 Documentation

- **[Architecture](docs/architecture.md)** - Core concepts and design
- **[Lineage Concepts](docs/lineage_concepts.md)** - Data lineage fundamentals  
- **[CLI Usage](docs/cli_usage.md)** - Complete command reference
- **[Configuration](docs/configuration.md)** - Advanced configuration options
- **[DBT Integration](docs/dbt_integration.md)** - Using with DBT projects
- **[OpenLineage Mapping](docs/openlineage_mapping.md)** - Output format specification
- **[Breaking Changes](docs/breaking_changes.md)** - Change detection and severity levels
- **[Advanced Use Cases](docs/advanced_use_cases.md)** - TVFs, stored procedures, and complex scenarios
- **[Edge Cases](docs/edge_cases.md)** - SELECT *, UNION, temp tables handling
- **[FAQ](docs/faq.md)** - Common questions and troubleshooting

## 🧪 Testing

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/test_parser.py     # Parser functionality
pytest tests/test_wildcard.py   # Wildcard selectors
pytest tests/test_adapter.py    # SQL dialect adapters

# Run with coverage
pytest --cov=infotracker --cov-report=html
```





## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [SQLGlot](https://github.com/tobymao/sqlglot) - SQL parsing library
- [OpenLineage](https://openlineage.io/) - Data lineage standard
- [Typer](https://typer.tiangolo.com/) - CLI framework
- [Rich](https://rich.readthedocs.io/) - Terminal formatting

---

**InfoTracker** - Making database schema evolution safer, one column at a time. 🎯 
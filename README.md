### InfoTracker

This is a Python CLI that extracts column-level lineage from SQL, runs impact analysis, and detects breaking changes. First adapter targets MS SQL.

#### For Students
Start with a simple command: `infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage`. This analyzes SQL files in the directory.

#### Setup & Installation
```bash
# Activate virtual environment first (REQUIRED)
source infotracker-env/bin/activate  # or your venv path

# Install dependencies
pip install -e .

# Verify installation
infotracker --help
```

#### Quickstart
```bash
# IMPORTANT: Always run InfoTracker commands in the activated virtual environment

# Extract lineage from all SQL files
infotracker extract --sql-dir examples/warehouse/sql --out-dir build/lineage

# Impact analysis (downstream dependencies)
infotracker impact -s dbo.fct_sales.Revenue+

# Impact analysis (upstream sources)
infotracker impact -s +dbo.Orders.OrderID

# Branch diff for breaking changes
infotracker diff --base main --head feature/x --sql-dir examples/warehouse/sql
```

#### Configuration
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
severity_threshold: BREAKING
```

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

#### Requirements
- Python 3.10+
- Virtual environment (activated)
- Basic SQL knowledge
- Git and shell

#### Troubleshooting
- **Error tracebacks on help commands**: Make sure you're running in an activated virtual environment
- **Command not found**: Activate your virtual environment first
- **Import errors**: Ensure all dependencies are installed with `pip install -e .`

#### License
MIT (or your team’s preferred license) 
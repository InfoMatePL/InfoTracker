### Configuration

#### YAML file (`infotracker.yml`)
```yaml
default_adapter: mssql
default_database: WarehouseDB
sql_dir: examples/warehouse/sql
out_dir: build/lineage
include: ["*.sql"]
exclude: ["*_wip.sql"]
severity_threshold: BREAKING
ignore:
  - "dbo.temp_*"
catalog: "catalog.yml"
```

#### How to use
- Pass `--config infotracker.yml` to any command
- If omitted, the tool looks for `infotracker.yml` in the repository root

#### Precedence
1. CLI flags (highest)
2. `infotracker.yml`
3. Built-in defaults

#### Notes
- Unknown objects resolve best-effort; provide a `catalog.yml` to improve type accuracy
- Deterministic outputs help CI diffs; set `severity_threshold` per your policy

#### dbt projects
- Run `dbt compile` first and point `sql_dir` to `target/compiled/<project>/models`
- Example:
  ```yaml
  sql_dir: target/compiled/my_dbt_project/models
  out_dir: build/lineage
  include: ["*.sql"]
  exclude: ["**/tests/**", "**/analysis/**", "**/snapshots/**"]
  ``` 
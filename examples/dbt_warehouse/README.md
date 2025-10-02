# dbt Warehouse Example Models

This directory contains example dbt-style SQL models demonstrating how InfoTracker works with dbt projects.

## Overview

These models represent **compiled dbt SQL** (no Jinja templating) converted from traditional T-SQL DDL/DML:
- **Models end with pure SELECT statements** - intermediate INSERT/UPDATE to #temp objects are allowed for complex transformations
- **Target object names** come from filenames (e.g., `dim_customer.sql` → `dim_customer` table/view)
- **Database/schema names are ignored** - since dbt object names are globally unique, all references are normalized to `DefaultDB.dbo.*` namespace
- **Temp table logic** converted to CTEs where possible
- **Procedure parameters** represented as commented dbt variables
- **No Jinja syntax** - these are examples of compiled SQL output from `dbt compile`

## Model Structure

### Sources & Seeds (01-04)
Base tables represented with schema-only SELECTs:
- `01_customers.sql` - Customer source data
- `02_orders.sql` - Orders source data
- `03_products.sql` - Products source data
- `04_order_items.sql` - Order items source data

### Staging Models (10-12)
Clean and normalize source data:
- `10_stg_orders.sql` - Orders with fulfillment flag
- `11_stg_order_items.sql` - Order items passthrough
- `12_stg_customers.sql` - Customers with email domain extraction

### Dimensions & Facts (20-41)
Core business entities:
- `20_vw_recent_orders.sql` - Recent orders (last 30 days) using CTE
- `30_dim_customer.sql` - Customer dimension
- `31_dim_product.sql` - Product dimension
- `40_fct_sales.sql` - Sales fact table
- `41_agg_sales_by_day.sql` - Daily sales aggregation

### Analytics Views (50-61)
Complex analytical queries:
- `50_vw_orders_all.sql` - All orders passthrough
- `51_vw_orders_all_enriched.sql` - Orders with recent flag
- `52_vw_order_details_star.sql` - Order details with wildcard expansion
- `53_vw_products_all.sql` - All products
- `54_vw_recent_orders_star_cte.sql` - Recent orders with CTE and wildcard
- `55_vw_orders_shipped_or_delivered.sql` - Filtered orders by status
- `56_vw_orders_union_star.sql` - Union of order views
- `60_vw_customer_order_analysis.sql` - Window functions (ranking, lag)
- `60_vw_customer_order_ranking.sql` - Customer order ranking
- `61_vw_sales_analytics.sql` - Sales analytics with window functions

### Advanced Models (90-96)
Converted from stored procedures/functions:
- `90_usp_refresh_sales_with_temp.sql` - Sales refresh with CTE staging
- `91_usp_snapshot_recent_orders_star.sql` - Recent orders snapshot
- `92_usp_rebuild_recent_sales_with_vars.sql` - Recent sales with variable logic
- `93_usp_top_products_since_var.sql` - Top 100 products calculation
- `94_fn_customer_orders_tvf.sql` - Table-valued function logic
- `95_usp_customer_metrics_dataset.sql` - Customer metrics calculation
- `96_demo_usage_tvf_and_proc.sql` - Complex workflow combining multiple models

## Running InfoTracker on dbt Models

### Extract lineage
```bash
infotracker extract --dbt \
  --sql-dir examples/dbt_warehouse/models \
  --out-dir build/dbt_lineage
```

### Impact analysis
```bash
infotracker impact --dbt \
  -s "+DefaultDB.dbo.fct_sales.Revenue+" \
  --graph-dir build/dbt_lineage \
  --max-depth 3
```

### Diff detection
```bash
infotracker diff --dbt \
  --base build/lineage_base \
  --head build/lineage_head \
  --format text
```

## Key dbt Concepts Demonstrated

### 1. Models end with pure SELECT
All models must end with a SELECT statement that defines the output. Intermediate INSERT/UPDATE to #temp objects are allowed for complex transformations, but the final statement must be pure SELECT. Materialization (table, view, incremental) is determined by dbt configuration, not SQL.

### 2. Filename = Model name
The filename (without .sql extension) becomes the model name:
- `dim_customer.sql` → `DefaultDB.dbo.dim_customer`

### 3. Globally unique object names
In dbt, object names are globally unique across the project. Database and schema names from SQL are ignored during parsing. All object references are normalized to `DefaultDB.dbo.*` regardless of what's written in the FROM clause.

### 4. CTEs replace temp tables
Temporary table logic from procedures is converted to CTEs:
```sql
-- Original procedure:
SELECT ... INTO #temp FROM ...
SELECT ... FROM #temp

-- dbt model:
WITH temp AS (
  SELECT ... FROM ...
)
SELECT ... FROM temp
```

### 5. Parameters become variables
Procedure parameters are represented as commented dbt variables:
```sql
-- WHERE CustomerID = @CustomerID
-- Becomes:
-- WHERE CustomerID = {{ var('customer_id') }}
```

## Column Lineage Examples

InfoTracker tracks column-level lineage through these transformations:

1. **Source → Staging**
   - `Orders.Status` → `stg_orders.IsFulfilled` (CASE transformation)

2. **Staging → Dimension**
   - `stg_customers.CustomerName` → `dim_customer.CustomerName` (passthrough)

3. **Dimension → Fact**
   - `stg_order_items.ExtendedPrice` → `fct_sales.Revenue` (rename)

4. **Fact → Analytics**
   - `fct_sales.Revenue` → `vw_sales_analytics.CustomerTotalRevenue` (SUM window function)

## Testing

To verify InfoTracker works correctly with these models:

```bash
# Extract lineage
infotracker extract --dbt \
  --sql-dir examples/dbt_warehouse/models \
  --out-dir build/dbt_lineage

# Check column graph
cat build/dbt_lineage/column_graph.json | jq '.nodes[] | select(.table_name=="fct_sales")'

# Verify impact analysis
infotracker impact --dbt \
  -s "DefaultDB.dbo.dim_customer.CustomerName" \
  --direction downstream \
  --graph-dir build/dbt_lineage
```

## Differences from Traditional SQL

| Traditional SQL | dbt Model (compiled) |
|----------------|-----------|
| `CREATE TABLE ...` | Schema-only `SELECT CAST(NULL AS type) AS column WHERE 1=0` |
| `CREATE VIEW ... AS SELECT` | Just the `SELECT` statement |
| `CREATE PROCEDURE ... AS BEGIN ... END` | Extract core `SELECT` logic (must end with SELECT) |
| Temp tables `#temp` | CTEs (or kept as #temp if needed for complex logic) |
| Variables `@var` | dbt variables `{{ var('var') }}` (compiled to actual values) |
| `Database.Schema.Table` | Ignored - normalized to `DefaultDB.dbo.Table` |
| `INSERT INTO ... EXEC proc` | `SELECT * FROM proc` (as model ref) |
| Jinja `{{ ref('model') }}` | Compiled to actual table name |

## Notes

- **IMPORTANT**: These models represent **compiled** dbt output (Jinja already rendered to pure SQL)
- InfoTracker does NOT support Jinja templating - you MUST run `dbt compile` first
- For production use, always run InfoTracker on `target/compiled/<project>/models/`
- Database and schema names are ignored since dbt object names are globally unique
- All object references are normalized to `DefaultDB.dbo.*` namespace
- Models must end with a pure SELECT statement (intermediate #temp operations are allowed)
- InfoTracker's `--dbt` mode handles these compiled dbt patterns correctly


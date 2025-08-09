### Column-level lineage: concepts

#### See the strands of power (aka where columns come from)
Put on your SQL spectacles and watch how outputs inherit their magic from inputs. It’s like genealogy, but for data—and fewer awkward family dinners.

- Trace: table → view → procedure → snapshot
- Decode: casts, cases, windows, and unions
- Outcome: confidence in what feeds what

If a node name has parentheses, quote it—Mermaid likes polite labels.

Column lineage explains how output columns are derived from input columns across tables, views, joins, and transforms.

#### Simple transform
```mermaid
graph TD
  A[Orders.OrderID] --> B[stg_orders.OrderID]
  C[Orders.CustomerID] --> D[stg_orders.CustomerID]
  E[Orders.OrderDate] --> F[stg_orders.OrderDate]
  G[Orders.OrderStatus] --> H[stg_orders.IsFulfilled]
```

#### Join + derived measure
```mermaid
graph TD
  OI_Q[OrderItems.Quantity] --> REV[fct_sales.Revenue]
  OI_P[OrderItems.UnitPrice] --> REV
  O_DT[Orders.OrderDate] --> FS_DT[fct_sales.OrderDate]
  OI_PID[OrderItems.ProductID] --> FS_PID[fct_sales.ProductID]
  O_CID[Orders.CustomerID] --> FS_CID[fct_sales.CustomerID]
```

Why it matters:
- Accurate impact analysis for safer changes
- Compliance and data governance
- Debugging data issues through source tracing 

#### Rename + cast example
SQL:
```sql
CREATE VIEW dbo.stg_orders_cast AS
SELECT
  o.OrderID AS id,
  CAST(o.OrderDate AS DATE) AS OrderDate
FROM dbo.Orders AS o;
```
Lineage:
```mermaid
graph TD
  OID[Orders.OrderID] --> SID[stg_orders_cast.id]
  ODT[Orders.OrderDate] --> SODT[stg_orders_cast.OrderDate]
```

#### CASE-derived column example
SQL:
```sql
CREATE VIEW dbo.stg_orders_flags AS
SELECT
  o.OrderID,
  CASE WHEN o.OrderStatus IN ('shipped','delivered') THEN 1 ELSE 0 END AS IsFulfilled
FROM dbo.Orders AS o;
```
Lineage:
```mermaid
graph TD
  OID2[Orders.OrderID] --> SOID2[stg_orders_flags.OrderID]
  OST[Orders.OrderStatus] --> SIF[stg_orders_flags.IsFulfilled]
```

#### Aggregation example
SQL:
```sql
CREATE VIEW dbo.agg_sales_by_day AS
SELECT
  CAST(o.OrderDate AS DATE) AS OrderDate,
  SUM(oi.Quantity) AS TotalQuantity,
  SUM(oi.Quantity * oi.UnitPrice) AS TotalRevenue
FROM dbo.stg_order_items AS oi
JOIN dbo.stg_orders AS o
  ON oi.OrderID = o.OrderID
GROUP BY CAST(o.OrderDate AS DATE);
```
Lineage:
```mermaid
graph TD
  ODT3[stg_orders.OrderDate] --> ADT[agg_sales_by_day.OrderDate]
  QTY[stg_order_items.Quantity] --> TQ[agg_sales_by_day.TotalQuantity]
  QTY2[stg_order_items.Quantity] --> TR[agg_sales_by_day.TotalRevenue]
  UPR[stg_order_items.UnitPrice] --> TR
```

#### Window function example
SQL:
```sql
CREATE VIEW dbo.vw_customer_order_rank AS
SELECT
  o.OrderID,
  o.CustomerID,
  ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate DESC) AS OrderRank
FROM dbo.stg_orders AS o;
```
Lineage:
```mermaid
graph TD
  OID4[stg_orders.OrderID] --> VID[vw_customer_order_rank.OrderID]
  CID4[stg_orders.CustomerID] --> VCID[vw_customer_order_rank.CustomerID]
  CID4 --> VRNK[vw_customer_order_rank.OrderRank]
  ODT4[stg_orders.OrderDate] --> VRNK
```

#### CTE + filter + projection example
SQL:
```sql
CREATE VIEW dbo.vw_recent_ids AS
WITH r AS (
  SELECT * FROM dbo.Orders WHERE OrderDate >= DATEADD(DAY,-30, CAST(GETDATE() AS DATE))
)
SELECT r.OrderID, r.CustomerID FROM r;
```
Lineage:
```mermaid
graph TD
  OID5[Orders.OrderID] --> RID[vw_recent_ids.OrderID]
  CID5[Orders.CustomerID] --> RCID[vw_recent_ids.CustomerID]
  ODT5[Orders.OrderDate] --> RID
  ODT5 --> RCID
```

#### UNION ALL alignment example
SQL:
```sql
CREATE VIEW dbo.vw_orders_union AS
SELECT OrderID, CustomerID FROM dbo.Orders WHERE OrderStatus = 'shipped'
UNION ALL
SELECT OrderID, CustomerID FROM dbo.Orders WHERE OrderStatus = 'delivered';
```
Lineage:
```mermaid
graph TD
  OID6A["Orders.OrderID (shipped)"] --> UOID[vw_orders_union.OrderID]
  OID6B["Orders.OrderID (delivered)"] --> UOID
  CID6A["Orders.CustomerID (shipped)"] --> UCID[vw_orders_union.CustomerID]
  CID6B["Orders.CustomerID (delivered)"] --> UCID
```

#### SELECT INTO snapshot example
SQL:
```sql
CREATE OR ALTER PROCEDURE dbo.usp_snapshot_orders AS
BEGIN
  SET NOCOUNT ON;
  IF OBJECT_ID('dbo.orders_snapshot','U') IS NULL
  BEGIN
    SELECT CAST(GETDATE() AS DATE) AS SnapshotDate, o.OrderID, o.CustomerID, o.OrderDate
    INTO dbo.orders_snapshot
    FROM dbo.vw_recent_orders_star_cte AS o;
  END
  ELSE
  BEGIN
    INSERT INTO dbo.orders_snapshot (SnapshotDate, OrderID, CustomerID, OrderDate)
    SELECT CAST(GETDATE() AS DATE), o.OrderID, o.CustomerID, o.OrderDate
    FROM dbo.vw_recent_orders_star_cte AS o;
  END
END;
```
Lineage (table outputs):
```mermaid
graph TD
  GDT["GETDATE() (constant)"] --> SSD[orders_snapshot.SnapshotDate]
  OID7[vw_recent_orders_star_cte.OrderID] --> SOID[orders_snapshot.OrderID]
  CID7[vw_recent_orders_star_cte.CustomerID] --> SCID[orders_snapshot.CustomerID]
  ODT7[vw_recent_orders_star_cte.OrderDate] --> SODT[orders_snapshot.OrderDate]
``` 

### Key definitions
- Source column: fully qualified `db.schema.object.column`
- Output column: column produced by a query/view/CTE/procedure step
- Expression: AST representing how an output is computed from inputs
- Lineage edge: relation from input column(s) to an output column

### Transformation taxonomy
- IDENTITY/RENAME: direct pass-through or alias rename
- CAST/TYPE_COERCION: explicit or implicit type changes
- ARITHMETIC: +, -, *, /, scalar math
- STRING_PARSE: SUBSTRING, LEFT/RIGHT, CONCAT, etc.
- CASE/IF: conditional branching
- AGGREGATION: SUM, AVG, COUNT, MIN, MAX with GROUP BY keys
- WINDOW: functions over partitions and orderings
- SET_OP: UNION/UNION ALL/INTERSECT/EXCEPT alignment by ordinal and type
- CONSTANT: literals and NOW/GETDATE
- JOIN: keys do not create columns, but affect nullability and cardinality of outputs

### Granularity and graph model
- Two-layer graph: object graph (nodes = objects), column graph (nodes = fully qualified columns)
- Column node id format: `db.schema.object.column`
- For CTEs and temp tables, scope nodes within the defining object; collapse once materialized

### Handling ambiguity
- Ambiguous column references (same name from multiple tables): require alias qualification; if missing, mark as ambiguous and report
- Function determinism: mark non-deterministic (e.g., GETDATE) as CONSTANT with note
- Unknown schema (e.g., external refs): emit placeholder schema and warn; allow user-supplied catalog

### OpenLineage mapping
- Dataset = object (`namespace` + `name`)
- `schema` facet: output fields and types
- `columnLineage` facet: per-field `inputFields`, `transformationType`, and `transformationDescription`
- Include concise expressions in descriptions; avoid full SQL dumps

### Naming and qualification rules
- MS SQL is case-insensitive; normalize internally but preserve display casing
- Prefer 3-part names `db.schema.object` in processing; allow `schema.object` and infer db if configured
- Avoid duplicate column names in outputs; if duplicates arise, qualify or rename deterministically 

### Direct vs indirect lineage (simple view)
- Direct lineage: the output column uses an input column directly.
  - Example: `SELECT o.OrderID AS id` → `id` comes straight from `Orders.OrderID`.
- Indirect lineage: the output value is affected by a column that is not shown in the output.
  - This happens with JOIN keys, WHERE filters, GROUP BY keys, and WINDOW partitions.

Small examples:
- JOIN (indirect):
  ```sql
  SELECT o.OrderID
  FROM dbo.Orders o
  INNER JOIN dbo.Customers c ON o.CustomerID = c.CustomerID;
  ```
  Here `Customers.CustomerID` does not appear in the SELECT, but it controls which orders survive the join. It is indirect lineage for `OrderID`.

- Filter (indirect):
  ```sql
  SELECT o.OrderID
  FROM dbo.Orders o
  WHERE o.OrderStatus = 'shipped';
  ```
  `OrderStatus` is not in the output, but it changes which rows exist. It is indirect lineage for `OrderID`.

Tip: when you change JOIN type (LEFT vs INNER) or filters, you change indirect lineage. That can change row counts, nullability, and downstream results. 
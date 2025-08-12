### Edge cases: SELECT * and star expansion

#### Boss fights (optional, but the loot is sweet)
Every adventure has bosses. Here, stars explode into many columns, UNIONs can misalign columns (wrong order), and temp tables vanish at dawn. Fear not.

- Strategy: resolve first, expand later
- Keep count: ordinals matter more than riddles
- Take notes: diff stars across branches to catch sneaky breaks

Roll for advantage by qualifying ambiguous columns.

Key idea: Build object-level lineage and resolve upstream schemas before expanding `*`.

#### Audience & prerequisites
- Audience: engineers handling dialect quirks and tricky constructs
- Prerequisites: SQL joins, set operations, and understanding of nullability/ordinals

### Tiny examples
- SELECT * in a view (prefer explicit):
  ```sql
  -- Not great
  CREATE VIEW dbo.v1 AS SELECT * FROM dbo.Orders;
  -- Better
  CREATE VIEW dbo.v1 AS SELECT OrderID, CustomerID, OrderDate FROM dbo.Orders;
  ```
- UNION ordinal mismatch:
  ```sql
  SELECT OrderID, CustomerID FROM dbo.Orders WHERE OrderStatus='shipped'
  UNION ALL
  SELECT CustomerID, OrderID FROM dbo.Orders WHERE OrderStatus='delivered'; -- wrong order
  ```
- SELECT INTO infers schema:
  ```sql
  SELECT OrderID, CustomerID INTO dbo.orders_copy FROM dbo.Orders;
  ```

### Do / Don’t
- Do: list columns explicitly in views; align UNION columns by position
- Don’t: rely on `SELECT *` in shared views; don’t reorder UNION columns silently

Included examples:
- `50_vw_orders_all.sql` — `SELECT *` from base table
- `51_vw_orders_all_enriched.sql` — `o.*` plus computed column
- `52_vw_order_details_star.sql` — `o.*` joined with specific columns
- `54_vw_recent_orders_star_cte.sql` — star inside CTE
- `55_vw_orders_shipped_or_delivered.sql` — filtered star
- `56_vw_orders_union_star.sql` — star with UNION ALL
- `91_usp_snapshot_recent_orders_star.sql` — star with SELECT INTO

Guidance:
- Resolve input schemas first; then expand `*`
- Track column order (UNION, SELECT INTO)
- Diff resolved star sets across branches to detect breaking changes 

### Star Expansion Example
Before: `SELECT * FROM students` (unknown columns)
After Resolution: Expands to `SELECT id, name, age FROM students`

| Before | After |
|--------|-------|
| * | id, name, age |

See docs/algorithm.md for more on resolution.

### Additional edge cases and handling
- Ambiguous column names without aliases
  - Handling: require qualification; emit warning and skip column lineage for ambiguous outputs
- Alias shadowing (output alias equals input alias)
  - Handling: resolve by scope; output alias has precedence only in projection
- Correlated subqueries and EXISTS/IN
  - Handling: propagate filter lineage to affected outputs; treat subquery outputs as influencing predicates
- Window frames (RANGE/ROWS BETWEEN)
  - Handling: lineage includes partition/order columns; mark transformation as WINDOW
- Set ops (INTERSECT/EXCEPT)
  - Handling: align by ordinal; types must be compatible; lineage merges inputs
- PIVOT/UNPIVOT
  - Handling: expanded columns; mark transformation as PIVOT; input columns may map to headers
- Dynamic SQL (EXEC with string)
  - Handling: v1 detect and warn as unsupported; offer hook for later instrumentation
- User-defined functions (scalar/table-valued)
  - Handling: treat scalar UDF as black-box function over its input columns; TVF as source object if schema known
- Data type coercion and overflow
  - Handling: note implicit casts; flag potential narrowing in diff
- Transactional temp tables reused across statements
  - Handling: track temp table schema across statements within procedure scope 

### More examples
- Window frame (indirect lineage drivers `k`, `dt`):
```sql
SELECT SUM(x) OVER (PARTITION BY k ORDER BY dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum
FROM t;
```
- TOP/ORDER BY (ordering column is indirect lineage):
```sql
SELECT TOP 1 OrderID FROM dbo.Orders ORDER BY OrderDate DESC;
```
`OrderDate` influences which row is selected; treat as indirect lineage for `OrderID`.

### See also
- `docs/algorithm.md`
- `docs/lineage_concepts.md`
- `docs/breaking_changes.md` 
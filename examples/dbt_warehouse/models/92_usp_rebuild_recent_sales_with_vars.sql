-- dbt model: usp_rebuild_recent_sales_with_vars
-- Recent sales rebuild with variable logic
-- Variables converted to CTEs

WITH max_order_date AS (
    SELECT CAST(MAX(o.OrderDate) AS DATE) AS maxOrderDate
    FROM DefaultDB.dbo.Orders AS o
),
recent_orders AS (
    SELECT
        o.OrderID,
        o.CustomerID,
        CAST(o.OrderDate AS DATE) AS OrderDate
    FROM DefaultDB.dbo.Orders AS o
    CROSS JOIN max_order_date AS mod
    WHERE o.OrderDate >= DATEADD(DAY, -14, mod.maxOrderDate)
)
SELECT
    r.OrderID,
    r.CustomerID,
    r.OrderDate,
    oi.ProductID,
    CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS Revenue
FROM recent_orders AS r
JOIN DefaultDB.dbo.OrderItems AS oi
  ON oi.OrderID = r.OrderID


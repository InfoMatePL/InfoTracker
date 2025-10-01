-- dbt model: usp_refresh_sales_with_temp
-- Sales refresh with temporary staging
-- In dbt, temp table logic becomes CTEs

WITH recent_orders AS (
    SELECT o.OrderID, o.CustomerID, o.OrderDate
    FROM DefaultDB.dbo.Orders AS o
    WHERE o.OrderDate >= DATEADD(DAY, -7, GETDATE())
),
sales AS (
    SELECT
        oi.OrderItemID AS SalesID,
        r.OrderDate,
        r.CustomerID,
        oi.ProductID,
        CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS Revenue
    FROM recent_orders AS r
    JOIN DefaultDB.dbo.OrderItems AS oi
      ON oi.OrderID = r.OrderID
)
SELECT
    CAST(GETDATE() AS DATE) AS SnapshotDate,
    s.SalesID,
    s.OrderDate,
    s.CustomerID,
    s.ProductID,
    s.Revenue
FROM sales AS s


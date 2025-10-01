-- dbt model: vw_recent_orders
-- View of recent orders (last 30 days) using CTE

WITH recent AS (
    SELECT
        o.OrderID,
        o.CustomerID,
        o.OrderDate
    FROM DefaultDB.dbo.stg_orders AS o
    WHERE o.OrderDate >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
)
SELECT
    r.OrderID,
    r.CustomerID,
    r.OrderDate
FROM recent AS r


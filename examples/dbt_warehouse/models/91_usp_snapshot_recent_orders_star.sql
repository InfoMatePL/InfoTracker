-- dbt model: usp_snapshot_recent_orders_star
-- Recent orders snapshot with wildcard expansion
-- Temp table logic converted to CTE

WITH ord AS (
    SELECT * FROM DefaultDB.dbo.vw_recent_orders_star_cte
)
SELECT 
    CAST(GETDATE() AS DATE) AS SnapshotDate, 
    o.OrderID, 
    o.CustomerID, 
    o.OrderDate, 
    o.Status
FROM ord AS o


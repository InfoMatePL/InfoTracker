CREATE VIEW EDW_CORE.dbo.vw_recent_orders AS
WITH recent AS (
    SELECT
        o.OrderID,
        o.CustomerID,
        o.OrderDate
    FROM STG.dbo.stg_orders AS o
    WHERE o.OrderDate >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
)
SELECT
    r.OrderID,
    r.CustomerID,
    r.OrderDate
FROM recent AS r; 
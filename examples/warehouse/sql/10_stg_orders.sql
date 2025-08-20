CREATE VIEW STG.dbo.stg_orders AS
SELECT
    o.OrderID,
    o.CustomerID,
    CAST(o.OrderDate AS DATE) AS OrderDate,
    CASE WHEN o.Status IN ('shipped','delivered') THEN 1 ELSE 0 END AS IsFulfilled
FROM STG.dbo.Orders AS o; 
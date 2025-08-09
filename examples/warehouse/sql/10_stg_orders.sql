CREATE VIEW dbo.stg_orders AS
SELECT
    o.OrderID,
    o.CustomerID,
    CAST(o.OrderDate AS DATE) AS OrderDate,
    CASE WHEN o.OrderStatus IN ('shipped', 'delivered') THEN 1 ELSE 0 END AS IsFulfilled
FROM dbo.Orders AS o; 
-- dbt model: stg_orders
-- Staging transformation on orders source

SELECT
    o.OrderID,
    o.CustomerID,
    CAST(o.OrderDate AS DATE) AS OrderDate,
    CASE WHEN o.Status IN ('shipped','delivered') THEN 1 ELSE 0 END AS IsFulfilled
FROM DefaultDB.dbo.Orders AS o


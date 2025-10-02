-- dbt model: vw_customer_order_ranking
-- Customer order ranking analysis

SELECT
    o.OrderID,
    o.CustomerID,
    o.OrderDate,
    RANK() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate DESC) AS OrderRank
FROM DefaultDB.dbo.Orders AS o


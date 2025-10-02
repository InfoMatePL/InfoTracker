-- dbt model: vw_orders_shipped_or_delivered
-- Orders filtered by status

SELECT *
FROM DefaultDB.dbo.Orders
WHERE Status IN ('shipped', 'delivered')


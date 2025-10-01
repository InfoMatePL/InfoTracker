-- dbt model: vw_order_details_star
-- Order details with wildcard expansion and join

SELECT
  o.*,
  oi.ProductID,
  oi.Quantity,
  oi.UnitPrice
FROM DefaultDB.dbo.vw_orders_all AS o
JOIN DefaultDB.dbo.OrderItems AS oi
  ON o.OrderID = oi.OrderID


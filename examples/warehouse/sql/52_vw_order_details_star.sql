CREATE VIEW EDW_CORE.dbo.vw_order_details_star AS
SELECT
  o.*,
  oi.ProductID,
  oi.Quantity,
  oi.UnitPrice
FROM EDW_CORE.dbo.vw_orders_all AS o
JOIN STG.dbo.OrderItems AS oi
  ON o.OrderID = oi.OrderID; 
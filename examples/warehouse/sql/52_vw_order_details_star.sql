CREATE VIEW dbo.vw_order_details_star AS
SELECT
  o.*,
  oi.ProductID,
  oi.Quantity,
  oi.UnitPrice
FROM dbo.vw_orders_all AS o
JOIN dbo.OrderItems AS oi
  ON o.OrderID = oi.OrderID; 
CREATE VIEW INFOMART.dbo.fct_sales AS
SELECT
    oi.OrderItemID AS SalesID,
    o.OrderDate,
    o.CustomerID,
    oi.ProductID,
    oi.Quantity,
    oi.UnitPrice,
    oi.ExtendedPrice AS Revenue
FROM STG.dbo.stg_order_items AS oi
JOIN STG.dbo.stg_orders AS o
  ON oi.OrderID = o.OrderID; 
-- dbt model: fct_sales
-- Sales fact table joining order items and orders

SELECT
    oi.OrderItemID AS SalesID,
    o.OrderDate,
    o.CustomerID,
    oi.ProductID,
    oi.Quantity,
    oi.UnitPrice,
    oi.ExtendedPrice AS Revenue
FROM DefaultDB.dbo.stg_order_items AS oi
JOIN DefaultDB.dbo.stg_orders AS o
  ON oi.OrderID = o.OrderID


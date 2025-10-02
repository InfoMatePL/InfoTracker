-- dbt model: stg_order_items
-- Staging transformation on order_items source

SELECT
    oi.OrderItemID,
    oi.OrderID,
    oi.ProductID,
    oi.Quantity,
    oi.UnitPrice,
    oi.ExtendedPrice
FROM DefaultDB.dbo.OrderItems AS oi


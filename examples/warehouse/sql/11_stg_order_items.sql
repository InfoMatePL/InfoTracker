CREATE VIEW STG.dbo.stg_order_items AS
SELECT
    oi.OrderItemID,
    oi.OrderID,
    oi.ProductID,
    oi.Quantity,
    oi.UnitPrice,
    oi.ExtendedPrice
FROM STG.dbo.OrderItems AS oi; 
CREATE VIEW dbo.stg_order_items AS
SELECT
    oi.OrderItemID,
    oi.OrderID,
    oi.ProductID,
    oi.Quantity,
    oi.UnitPrice,
    CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS ExtendedPrice
FROM dbo.OrderItems AS oi; 
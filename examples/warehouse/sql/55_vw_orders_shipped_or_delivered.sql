CREATE VIEW dbo.vw_orders_shipped_or_delivered AS
SELECT *
FROM dbo.Orders
WHERE OrderStatus IN ('shipped','delivered'); 
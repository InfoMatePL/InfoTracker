CREATE VIEW EDW_CORE.dbo.vw_orders_shipped_or_delivered AS
SELECT *
FROM STG.dbo.Orders
WHERE Status IN ('shipped','delivered'); 
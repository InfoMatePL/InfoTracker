CREATE VIEW EDW_CORE.dbo.vw_orders_union_star AS
SELECT * FROM STG.dbo.Orders WHERE Status = 'shipped'
UNION ALL
SELECT * FROM STG.dbo.Orders WHERE Status = 'delivered'; 
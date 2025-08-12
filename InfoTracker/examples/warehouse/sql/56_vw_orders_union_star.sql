CREATE VIEW dbo.vw_orders_union_star AS
SELECT * FROM dbo.Orders WHERE OrderStatus = 'shipped'
UNION ALL
SELECT * FROM dbo.Orders WHERE OrderStatus = 'delivered'; 
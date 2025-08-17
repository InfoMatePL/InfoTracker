CREATE VIEW STG.dbo.stg_orders AS
SELECT
    o.OrderID,
    o.CustomerID,
    o.OrderDate,
    o.Status
FROM STG.dbo.Orders AS o; 
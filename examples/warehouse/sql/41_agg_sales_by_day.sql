CREATE VIEW INFOMART.dbo.agg_sales_by_day AS
SELECT
    CAST(o.OrderDate AS DATE) AS OrderDate,
    SUM(oi.Quantity) AS TotalQuantity,
    SUM(oi.ExtendedPrice) AS TotalRevenue
FROM STG.dbo.stg_order_items AS oi
JOIN STG.dbo.stg_orders AS o
  ON oi.OrderID = o.OrderID
GROUP BY CAST(o.OrderDate AS DATE); 
CREATE VIEW dbo.agg_sales_by_day AS
SELECT
    CAST(o.OrderDate AS DATE) AS OrderDate,
    SUM(oi.Quantity) AS TotalQuantity,
    SUM(oi.ExtendedPrice) AS TotalRevenue
FROM dbo.stg_order_items AS oi
JOIN dbo.stg_orders AS o
  ON oi.OrderID = o.OrderID
GROUP BY CAST(o.OrderDate AS DATE); 
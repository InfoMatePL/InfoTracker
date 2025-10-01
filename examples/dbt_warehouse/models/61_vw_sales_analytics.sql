-- dbt model: vw_sales_analytics
-- Sales analytics with window functions

SELECT
    s.SalesID,
    s.CustomerID,
    s.ProductID,
    s.OrderDate,
    s.Revenue,
    SUM(s.Revenue) OVER (PARTITION BY s.CustomerID) AS CustomerTotalRevenue,
    AVG(s.Revenue) OVER (PARTITION BY s.ProductID ORDER BY s.OrderDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MovingAvgRevenue,
    DENSE_RANK() OVER (PARTITION BY s.CustomerID ORDER BY s.Revenue DESC) AS CustomerRevenueRank
FROM DefaultDB.dbo.fct_sales AS s


-- Window function example: Ranking and lag functions
CREATE VIEW INFOMART.dbo.vw_customer_order_analysis AS
SELECT 
    OrderID,
    CustomerID,
    OrderDate,
    Status AS TotalAmount,
    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate) as customer_order_rank,
    LAG(OrderID, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) as prev_order_amount,
    COUNT(*) OVER (PARTITION BY CustomerID) as customer_lifetime_value,
    AVG(OrderID) OVER (PARTITION BY CustomerID ORDER BY OrderDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_avg_amount
FROM STG.dbo.Orders

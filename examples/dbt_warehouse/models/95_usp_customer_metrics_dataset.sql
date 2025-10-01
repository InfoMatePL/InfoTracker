-- dbt model: usp_customer_metrics_dataset
-- Customer metrics calculation
-- Procedure parameters would be dbt variables/macros

SELECT
    c.CustomerID,
    c.CustomerName,
    c.CustomerType,
    c.RegistrationDate,
    COUNT(DISTINCT o.OrderID) AS TotalOrders,
    COUNT(DISTINCT oi.ProductID) AS UniqueProductsPurchased,
    SUM(oi.Quantity) AS TotalItemsPurchased,
    SUM(CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2))) AS TotalRevenue,
    AVG(CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2))) AS AverageOrderValue,
    MAX(o.OrderDate) AS LastOrderDate,
    DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) AS DaysSinceLastOrder,
    CASE 
        WHEN MAX(o.OrderDate) >= DATEADD(MONTH, -1, GETDATE()) THEN 'Active'
        WHEN MAX(o.OrderDate) >= DATEADD(MONTH, -3, GETDATE()) THEN 'Recent'
        WHEN MAX(o.OrderDate) >= DATEADD(MONTH, -6, GETDATE()) THEN 'Occasional'
        ELSE 'Inactive'
    END AS CustomerActivityStatus,
    CASE 
        WHEN COUNT(DISTINCT o.OrderID) > 1 THEN
            CAST(COUNT(DISTINCT o.OrderID) AS FLOAT) / 
            NULLIF(DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)), 0)
        ELSE 0 
    END AS OrdersPerDay
FROM DefaultDB.dbo.Customers AS c
LEFT JOIN DefaultDB.dbo.Orders AS o ON c.CustomerID = o.CustomerID
LEFT JOIN DefaultDB.dbo.OrderItems AS oi ON o.OrderID = oi.OrderID
-- WHERE (@CustomerID IS NULL OR c.CustomerID = @CustomerID)  -- Would be: {{ var('customer_id', none) }}
--   AND (@IncludeInactive = 1 OR c.IsActive = 1)
--   AND (o.OrderDate IS NULL OR o.OrderDate BETWEEN @StartDate AND @EndDate)
GROUP BY 
    c.CustomerID, 
    c.CustomerName, 
    c.CustomerType, 
    c.RegistrationDate
HAVING COUNT(DISTINCT o.OrderID) > 0
ORDER BY TotalRevenue DESC, TotalOrders DESC


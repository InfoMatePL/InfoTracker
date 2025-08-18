-- Procedure that returns a dataset
-- This procedure does calculations on existing tables and views, 
-- finishing with a single SELECT statement that can be captured
-- into a temp table for further processing

CREATE OR ALTER PROCEDURE dbo.usp_customer_metrics_dataset
    @CustomerID INT = NULL,
    @StartDate DATE = NULL,
    @EndDate DATE = NULL,
    @IncludeInactive BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Set default parameters if not provided
    IF @StartDate IS NULL SET @StartDate = DATEADD(MONTH, -3, GETDATE());
    IF @EndDate IS NULL SET @EndDate = GETDATE();
    
    -- Build dynamic WHERE clause based on parameters
    DECLARE @WhereClause NVARCHAR(MAX) = 'WHERE 1=1';
    IF @CustomerID IS NOT NULL 
        SET @WhereClause = @WhereClause + ' AND c.CustomerID = ' + CAST(@CustomerID AS VARCHAR(10));
    IF @IncludeInactive = 0 
        SET @WhereClause = @WhereClause + ' AND c.IsActive = 1';
    
    -- Main calculation query that returns the dataset
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
            ELSE 'Inactive'
        END AS CustomerActivityStatus,
        CASE
            WHEN SUM(CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2))) >= 10000 THEN 'High Value'
            WHEN SUM(CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2))) >= 5000 THEN 'Medium Value'
            ELSE 'Standard'
        END AS CustomerTier
    FROM dbo.Customers AS c
    LEFT JOIN dbo.Orders AS o ON c.CustomerID = o.CustomerID
    LEFT JOIN dbo.OrderItems AS oi ON o.OrderID = oi.OrderID
    WHERE (@CustomerID IS NULL OR c.CustomerID = @CustomerID)
      AND (@IncludeInactive = 1 OR c.IsActive = 1)
      AND (o.OrderDate IS NULL OR o.OrderDate BETWEEN @StartDate AND @EndDate)
    GROUP BY c.CustomerID, c.CustomerName, c.CustomerType, c.RegistrationDate
    HAVING COUNT(DISTINCT o.OrderID) > 0
    ORDER BY TotalRevenue DESC;
END;

-- Demonstration of usage patterns for the new tabular function and procedure
-- Shows how they can be used together in a workflow

-- Example 1: Using the inline table-valued function
-- This demonstrates how the function can be used like a view in lineage
SELECT 
    f.*,
    p.Category,
    p.Name AS ProductName
FROM dbo.fn_customer_orders_inline(1, '2024-01-01', '2024-12-31') AS f
INNER JOIN dbo.Products AS p ON f.ProductID = p.ProductID
WHERE f.IsFulfilled = 1;

-- Example 2: Using the multi-statement table-valued function
-- This shows the more complex function with additional computed columns
SELECT 
    f.*,
    c.CustomerName,
    c.CustomerType
FROM dbo.fn_customer_orders_mstvf(1, '2024-01-01', '2024-12-31') AS f
INNER JOIN dbo.Customers AS c ON f.CustomerID = c.CustomerID
WHERE f.DaysSinceOrder <= 30;

-- Example 3: Using the procedure with EXEC into temp table
-- This demonstrates the procedure returning a dataset that can be captured
-- and used further in downstream operations

-- Create temp table to capture procedure output
IF OBJECT_ID('tempdb..#customer_metrics') IS NOT NULL DROP TABLE #customer_metrics;

-- Execute procedure and capture results into temp table
INSERT INTO #customer_metrics
EXEC dbo.usp_customer_metrics_dataset 
    @CustomerID = NULL,  -- All customers
    @StartDate = '2024-01-01',
    @EndDate = '2024-12-31',
    @IncludeInactive = 0;

-- Use the temp table for further processing
-- This shows how the procedure output becomes an input to other operations
SELECT 
    cm.*,
    CASE 
        WHEN cm.TotalRevenue >= 10000 THEN 'High Value'
        WHEN cm.TotalRevenue >= 5000 THEN 'Medium Value'
        ELSE 'Standard'
    END AS CustomerTier,
    ROW_NUMBER() OVER (ORDER BY cm.TotalRevenue DESC) AS RevenueRank
FROM #customer_metrics AS cm
WHERE cm.CustomerActivityStatus IN ('Active', 'Recent');

-- Example 4: Combining function and procedure outputs
-- This demonstrates a more complex workflow using both objects
SELECT 
    f.CustomerID,
    f.OrderID,
    f.ProductID,
    f.ExtendedPrice,
    cm.TotalRevenue,
    cm.CustomerActivityStatus,
    cm.CustomerTier,
    CAST(f.ExtendedPrice / NULLIF(cm.TotalRevenue, 0) * 100 AS DECIMAL(5,2)) AS OrderContributionPercent
FROM dbo.fn_customer_orders_inline(1, '2024-01-01', '2024-12-31') AS f
INNER JOIN #customer_metrics AS cm ON f.CustomerID = cm.CustomerID
WHERE cm.CustomerActivityStatus = 'Active';

-- Example 5: Insert procedure results into a permanent table
-- This shows the end-to-end lineage from procedure to target table
IF OBJECT_ID('dbo.customer_metrics_archive', 'U') IS NULL
BEGIN
    SELECT 
        GETDATE() AS ArchiveDate,
        cm.*
    INTO dbo.customer_metrics_archive
    FROM #customer_metrics AS cm;
END
ELSE
BEGIN
    INSERT INTO dbo.customer_metrics_archive (
        ArchiveDate, CustomerID, CustomerName, CustomerType, RegistrationDate,
        TotalOrders, UniqueProductsPurchased, TotalItemsPurchased, TotalRevenue,
        AverageOrderValue, LastOrderDate, DaysSinceLastOrder, CustomerActivityStatus, OrdersPerDay
    )
    SELECT 
        GETDATE(),
        cm.*
    FROM #customer_metrics AS cm;
END;

-- Clean up
DROP TABLE #customer_metrics;

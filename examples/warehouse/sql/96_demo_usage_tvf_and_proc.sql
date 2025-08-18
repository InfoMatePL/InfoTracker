-- Demo usage of TVF and procedure with EXEC into temp tables
-- This shows how functions and procedures can be used together
-- and how EXEC results can be captured for further processing

CREATE OR ALTER PROCEDURE dbo.demo_usage_tvf_and_proc
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Step 1: Use tabular function in a regular query
    SELECT 
        f.OrderID,
        f.CustomerID,
        f.ExtendedPrice,
        f.PriceCategory,
        p.ProductName,
        p.Category
    INTO #function_results
    FROM dbo.fn_customer_orders_inline(1, '2024-01-01', '2024-12-31') AS f
    INNER JOIN dbo.Products AS p ON f.ProductID = p.ProductID
    WHERE f.ExtendedPrice > 100;
    
    -- Step 2: Execute procedure and capture results into temp table
    IF OBJECT_ID('tempdb..#customer_metrics') IS NOT NULL 
        DROP TABLE #customer_metrics;
    
    -- Create temp table structure to match procedure output
    CREATE TABLE #customer_metrics (
        CustomerID INT,
        CustomerName NVARCHAR(100),
        CustomerType NVARCHAR(50),
        RegistrationDate DATE,
        TotalOrders INT,
        UniqueProductsPurchased INT,
        TotalItemsPurchased INT,
        TotalRevenue DECIMAL(18,2),
        AverageOrderValue DECIMAL(18,2),
        LastOrderDate DATE,
        DaysSinceLastOrder INT,
        CustomerActivityStatus NVARCHAR(20),
        CustomerTier NVARCHAR(20)
    );
    
    -- Execute procedure and insert results
    INSERT INTO #customer_metrics
    EXEC dbo.usp_customer_metrics_dataset 
        @CustomerID = NULL,
        @StartDate = '2024-01-01',
        @EndDate = '2024-12-31',
        @IncludeInactive = 0;
    
    -- Step 3: Combine function and procedure results for final analysis
    SELECT 
        f.CustomerID,
        f.OrderID,
        f.ExtendedPrice AS OrderValue,
        cm.TotalRevenue AS CustomerTotalRevenue,
        cm.CustomerTier,
        cm.CustomerActivityStatus,
        CAST(f.ExtendedPrice / NULLIF(cm.TotalRevenue, 0) * 100 AS DECIMAL(5,2)) AS OrderContributionPercent,
        CASE 
            WHEN f.ExtendedPrice > cm.AverageOrderValue * 1.5 THEN 'Above Average'
            WHEN f.ExtendedPrice < cm.AverageOrderValue * 0.5 THEN 'Below Average'
            ELSE 'Average'
        END AS OrderValueCategory
    INTO #final_analysis
    FROM #function_results AS f
    INNER JOIN #customer_metrics AS cm ON f.CustomerID = cm.CustomerID;
    
    -- Step 4: Final output with enriched analysis
    SELECT
        fa.*,
        ROW_NUMBER() OVER (PARTITION BY fa.CustomerID ORDER BY fa.OrderValue DESC) AS CustomerOrderRank,
        DENSE_RANK() OVER (ORDER BY fa.CustomerTotalRevenue DESC) AS CustomerRevenueRank
    FROM #final_analysis AS fa
    ORDER BY fa.CustomerTotalRevenue DESC, fa.OrderValue DESC;
    
    -- Cleanup
    DROP TABLE #function_results;
    DROP TABLE #customer_metrics;
    DROP TABLE #final_analysis;
END;

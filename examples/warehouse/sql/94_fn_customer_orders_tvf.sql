-- Tabular Function Examples for InfoTracker TVF/Procedure Lineage Demo
-- This file demonstrates both inline and multi-statement table-valued functions

-- Inline Table-Valued Function (RETURN AS)
CREATE FUNCTION dbo.fn_customer_orders_inline
(
    @CustomerID INT,
    @StartDate DATE,
    @EndDate DATE
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        oi.ProductID,
        oi.Quantity,
        oi.UnitPrice,
        CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS ExtendedPrice,
        CASE 
            WHEN oi.UnitPrice > 100 THEN 'Premium'
            WHEN oi.UnitPrice > 50 THEN 'Standard'
            ELSE 'Budget'
        END AS PriceCategory
    FROM dbo.Orders AS o
    INNER JOIN dbo.OrderItems AS oi ON o.OrderID = oi.OrderID
    WHERE o.CustomerID = @CustomerID
      AND o.OrderDate BETWEEN @StartDate AND @EndDate
);
GO

-- Multi-Statement Table-Valued Function (RETURN TABLE)
CREATE FUNCTION dbo.fn_customer_orders_mstvf
(
    @CustomerID INT,
    @StartDate DATE,
    @EndDate DATE
)
RETURNS @Result TABLE
(
    OrderID INT,
    CustomerID INT,
    OrderDate DATE,
    ProductID INT,
    ExtendedPrice DECIMAL(18,2),
    DaysSinceOrder INT,
    OrderRank INT
)
AS
BEGIN
    -- Insert base data
    INSERT INTO @Result (OrderID, CustomerID, OrderDate, ProductID, ExtendedPrice, DaysSinceOrder)
    SELECT
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        oi.ProductID,
        CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS ExtendedPrice,
        DATEDIFF(DAY, o.OrderDate, GETDATE()) AS DaysSinceOrder
    FROM dbo.Orders AS o
    INNER JOIN dbo.OrderItems AS oi ON o.OrderID = oi.OrderID
    WHERE o.CustomerID = @CustomerID
      AND o.OrderDate BETWEEN @StartDate AND @EndDate;
    
    -- Update with ranking
    UPDATE @Result
    SET OrderRank = (
        SELECT COUNT(*) + 1
        FROM @Result r2
        WHERE r2.ExtendedPrice > [@Result].ExtendedPrice
    );
    
    RETURN;
END;

-- Parametrized Tabular Function with two syntax variants
-- This function leverages underlying structure to calculate customer order metrics
-- and returns a table result set that should be visible as a view in lineage

-- Variant 1: RETURN AS (inline table-valued function)
CREATE OR ALTER FUNCTION dbo.fn_customer_orders_inline
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
        o.OrderStatus,
        oi.ProductID,
        oi.Quantity,
        oi.UnitPrice,
        CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS ExtendedPrice,
        CASE 
            WHEN o.OrderStatus IN ('shipped', 'delivered') THEN 1 
            ELSE 0 
        END AS IsFulfilled
    FROM dbo.Orders AS o
    INNER JOIN dbo.OrderItems AS oi ON o.OrderID = oi.OrderID
    WHERE o.CustomerID = @CustomerID
      AND o.OrderDate BETWEEN @StartDate AND @EndDate
);

-- Variant 2: RETURN TABLE (multi-statement table-valued function)
CREATE OR ALTER FUNCTION dbo.fn_customer_orders_mstvf
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
    OrderStatus VARCHAR(50),
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    ExtendedPrice DECIMAL(18,2),
    IsFulfilled BIT,
    DaysSinceOrder INT
)
AS
BEGIN
    INSERT INTO @Result
    SELECT
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        o.OrderStatus,
        oi.ProductID,
        oi.Quantity,
        oi.UnitPrice,
        CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS ExtendedPrice,
        CASE 
            WHEN o.OrderStatus IN ('shipped', 'delivered') THEN 1 
            ELSE 0 
        END AS IsFulfilled,
        DATEDIFF(DAY, o.OrderDate, GETDATE()) AS DaysSinceOrder
    FROM dbo.Orders AS o
    INNER JOIN dbo.OrderItems AS oi ON o.OrderID = oi.OrderID
    WHERE o.CustomerID = @CustomerID
      AND o.OrderDate BETWEEN @StartDate AND @EndDate;
    
    RETURN;
END;

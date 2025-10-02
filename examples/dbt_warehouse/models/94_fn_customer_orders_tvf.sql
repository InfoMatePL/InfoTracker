-- dbt model: fn_customer_orders_tvf
-- Table-valued function converted to parameterized model
-- In dbt, parameters would come from macros/variables
-- This represents the inline table-valued function logic

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
FROM DefaultDB.dbo.Orders AS o
INNER JOIN DefaultDB.dbo.OrderItems AS oi ON o.OrderID = oi.OrderID
-- WHERE o.CustomerID = @CustomerID  -- Would be dbt variable: {{ var('customer_id') }}
--   AND o.OrderDate BETWEEN @StartDate AND @EndDate


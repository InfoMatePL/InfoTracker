-- dbt model: usp_top_products_since_var
-- Top products calculation with variable
-- Variable logic converted to CTE

WITH since_date AS (
    SELECT DATEADD(DAY, -30, CAST(MAX(o.OrderDate) AS DATE)) AS since
    FROM DefaultDB.dbo.Orders AS o
)
SELECT TOP 100
    oi.ProductID,
    SUM(oi.Quantity) AS TotalQty,
    SUM(oi.Quantity * oi.UnitPrice) AS TotalRevenue
FROM DefaultDB.dbo.OrderItems AS oi
JOIN DefaultDB.dbo.Orders AS o
  ON o.OrderID = oi.OrderID
CROSS JOIN since_date AS sd
WHERE o.OrderDate >= sd.since
GROUP BY oi.ProductID
ORDER BY TotalRevenue DESC


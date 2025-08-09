CREATE OR ALTER PROCEDURE dbo.usp_top_products_since_var
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @since DATE;
    SELECT @since = DATEADD(DAY, -30, CAST(MAX(o.OrderDate) AS DATE))
    FROM dbo.Orders AS o;

    IF OBJECT_ID('dbo.top_products_last30_var', 'U') IS NULL
    BEGIN
        SELECT TOP 100
            oi.ProductID,
            SUM(oi.Quantity) AS TotalQty,
            SUM(oi.Quantity * oi.UnitPrice) AS TotalRevenue
        INTO dbo.top_products_last30_var
        FROM dbo.OrderItems AS oi
        JOIN dbo.Orders AS o
          ON o.OrderID = oi.OrderID
        WHERE o.OrderDate >= @since
        GROUP BY oi.ProductID
        ORDER BY TotalRevenue DESC;
    END
    ELSE
    BEGIN
        TRUNCATE TABLE dbo.top_products_last30_var;
        INSERT INTO dbo.top_products_last30_var (
            ProductID, TotalQty, TotalRevenue
        )
        SELECT TOP 100
            oi.ProductID,
            SUM(oi.Quantity),
            SUM(oi.Quantity * oi.UnitPrice)
        FROM dbo.OrderItems AS oi
        JOIN dbo.Orders AS o
          ON o.OrderID = oi.OrderID
        WHERE o.OrderDate >= @since
        GROUP BY oi.ProductID
        ORDER BY SUM(oi.Quantity * oi.UnitPrice) DESC;
    END
END; 
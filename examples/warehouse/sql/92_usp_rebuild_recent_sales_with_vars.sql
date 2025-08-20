CREATE OR ALTER PROCEDURE INFOMART.dbo.usp_rebuild_recent_sales_with_vars
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @maxOrderDate DATE;
    SELECT @maxOrderDate = CAST(MAX(o.OrderDate) AS DATE)
    FROM STG.dbo.Orders AS o;

    IF OBJECT_ID('tempdb..#recent_orders', 'U') IS NOT NULL DROP TABLE #recent_orders;
    SELECT
        o.OrderID,
        o.CustomerID,
        CAST(o.OrderDate AS DATE) AS OrderDate
    INTO #recent_orders
    FROM STG.dbo.Orders AS o
    WHERE o.OrderDate >= DATEADD(DAY, -14, @maxOrderDate);

    IF OBJECT_ID('INFOMART.dbo.fct_sales_recent_var', 'U') IS NULL
    BEGIN
        SELECT
            r.OrderID,
            r.CustomerID,
            r.OrderDate,
            oi.ProductID,
            CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS Revenue
        INTO INFOMART.dbo.fct_sales_recent_var
        FROM #recent_orders AS r
        JOIN STG.dbo.OrderItems AS oi
          ON oi.OrderID = r.OrderID;
    END
    ELSE
    BEGIN
        TRUNCATE TABLE INFOMART.dbo.fct_sales_recent_var;
        INSERT INTO INFOMART.dbo.fct_sales_recent_var (
            OrderID, CustomerID, OrderDate, ProductID, Revenue
        )
        SELECT
            r.OrderID,
            r.CustomerID,
            r.OrderDate,
            oi.ProductID,
            CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2))
        FROM #recent_orders AS r
        JOIN STG.dbo.OrderItems AS oi
          ON oi.OrderID = r.OrderID;
    END
END; 
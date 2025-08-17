CREATE OR ALTER PROCEDURE INFOMART.dbo.usp_refresh_sales_with_temp
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @snapshotDate DATE = CAST(GETDATE() AS DATE);

    IF OBJECT_ID('tempdb..#recent_orders') IS NOT NULL DROP TABLE #recent_orders;
    SELECT o.OrderID, o.CustomerID, o.OrderDate
    INTO #recent_orders
    FROM STG.dbo.Orders AS o
    WHERE o.OrderDate >= DATEADD(DAY, -7, GETDATE());

    IF OBJECT_ID('tempdb..#sales') IS NOT NULL DROP TABLE #sales;
    SELECT
        oi.OrderItemID AS SalesID,
        r.OrderDate,
        r.CustomerID,
        oi.ProductID,
        CAST(oi.Quantity * oi.UnitPrice AS DECIMAL(18,2)) AS Revenue
    INTO #sales
    FROM #recent_orders AS r
    JOIN STG.dbo.OrderItems AS oi
      ON oi.OrderID = r.OrderID;

    IF OBJECT_ID('INFOMART.dbo.fct_sales_snapshot', 'U') IS NULL
    BEGIN
        SELECT
            @snapshotDate AS SnapshotDate,
            s.SalesID,
            s.OrderDate,
            s.CustomerID,
            s.ProductID,
            s.Revenue
        INTO INFOMART.dbo.fct_sales_snapshot
        FROM #sales AS s;
    END
    ELSE
    BEGIN
        DELETE FROM INFOMART.dbo.fct_sales_snapshot WHERE SnapshotDate = @snapshotDate;
        INSERT INTO INFOMART.dbo.fct_sales_snapshot (
            SnapshotDate, SalesID, OrderDate, CustomerID, ProductID, Revenue
        )
        SELECT
            @snapshotDate,
            s.SalesID,
            s.OrderDate,
            s.CustomerID,
            s.ProductID,
            s.Revenue
        FROM #sales AS s;
    END
END; 
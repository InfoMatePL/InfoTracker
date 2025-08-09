CREATE OR ALTER PROCEDURE dbo.usp_snapshot_recent_orders_star
AS
BEGIN
  SET NOCOUNT ON;

  IF OBJECT_ID('tempdb..#ord') IS NOT NULL DROP TABLE #ord;
  SELECT * INTO #ord FROM dbo.vw_recent_orders_star_cte;

  IF OBJECT_ID('dbo.orders_recent_snapshot','U') IS NULL
  BEGIN
    SELECT CAST(GETDATE() AS DATE) AS SnapshotDate, o.*
    INTO dbo.orders_recent_snapshot
    FROM #ord AS o;
  END
  ELSE
  BEGIN
    INSERT INTO dbo.orders_recent_snapshot (
      SnapshotDate, OrderID, CustomerID, OrderDate, OrderStatus
    )
    SELECT CAST(GETDATE() AS DATE), o.OrderID, o.CustomerID, o.OrderDate, o.OrderStatus
    FROM #ord AS o;
  END
END; 
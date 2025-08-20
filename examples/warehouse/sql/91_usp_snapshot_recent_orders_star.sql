CREATE OR ALTER PROCEDURE INFOMART.dbo.usp_snapshot_recent_orders_star
AS
BEGIN
  SET NOCOUNT ON;

  IF OBJECT_ID('tempdb..#ord') IS NOT NULL DROP TABLE #ord;
  SELECT * INTO #ord FROM EDW_CORE.dbo.vw_recent_orders_star_cte;

  IF OBJECT_ID('INFOMART.dbo.orders_recent_snapshot','U') IS NULL
  BEGIN
    SELECT CAST(GETDATE() AS DATE) AS SnapshotDate, o.*
    INTO INFOMART.dbo.orders_recent_snapshot
    FROM #ord AS o;
  END
  ELSE
  BEGIN
    INSERT INTO INFOMART.dbo.orders_recent_snapshot (
      SnapshotDate, OrderID, CustomerID, OrderDate, Status
    )
    SELECT CAST(GETDATE() AS DATE), o.OrderID, o.CustomerID, o.OrderDate, o.Status
    FROM #ord AS o;
  END
END; 
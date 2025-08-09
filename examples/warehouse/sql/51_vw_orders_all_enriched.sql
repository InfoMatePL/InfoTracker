CREATE VIEW dbo.vw_orders_all_enriched AS
SELECT
  o.*,
  CASE WHEN o.OrderDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END AS IsRecent
FROM dbo.vw_orders_all AS o; 
-- dbt model: vw_orders_all_enriched
-- Enriched orders with recent flag

SELECT
  o.*,
  CASE WHEN o.OrderDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END AS IsRecent
FROM DefaultDB.dbo.vw_orders_all AS o


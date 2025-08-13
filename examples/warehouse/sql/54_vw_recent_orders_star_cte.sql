CREATE VIEW dbo.vw_recent_orders_star_cte AS
WITH r AS (
  SELECT *
  FROM dbo.vw_orders_all
  WHERE OrderDate >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
)
SELECT * FROM r; 
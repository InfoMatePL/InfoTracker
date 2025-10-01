-- dbt model: vw_recent_orders_star_cte
-- Recent orders using CTE with wildcard

WITH r AS (
  SELECT *
  FROM DefaultDB.dbo.vw_orders_all
  WHERE OrderDate >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
)
SELECT * FROM r


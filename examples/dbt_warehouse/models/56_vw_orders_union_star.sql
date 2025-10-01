-- dbt model: vw_orders_union_star
-- Union of different order views

SELECT * FROM DefaultDB.dbo.vw_recent_orders_star_cte
UNION ALL
SELECT * FROM DefaultDB.dbo.vw_orders_shipped_or_delivered


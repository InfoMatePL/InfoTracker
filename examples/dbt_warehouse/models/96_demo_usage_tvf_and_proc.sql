-- dbt model: demo_usage_tvf_and_proc
-- Demonstration of complex workflow
-- Combines function and procedure outputs
-- In dbt, EXEC pattern becomes ref() to upstream models

WITH customer_metrics AS (
    SELECT * FROM DefaultDB.dbo.usp_customer_metrics_dataset
),
customer_orders AS (
    SELECT * FROM DefaultDB.dbo.fn_customer_orders_inline
),
enriched_metrics AS (
    SELECT 
        cm.*,
        CASE 
            WHEN cm.TotalRevenue >= 10000 THEN 'High Value'
            WHEN cm.TotalRevenue >= 5000 THEN 'Medium Value'
            ELSE 'Standard'
        END AS CustomerTier,
        ROW_NUMBER() OVER (ORDER BY cm.TotalRevenue DESC) AS RevenueRank
    FROM customer_metrics AS cm
    WHERE cm.CustomerActivityStatus IN ('Active', 'Recent')
)
SELECT 
    f.CustomerID,
    f.OrderID,
    f.ProductID,
    f.ExtendedPrice,
    em.TotalRevenue,
    em.CustomerActivityStatus,
    em.CustomerTier,
    CAST(f.ExtendedPrice / NULLIF(em.TotalRevenue, 0) * 100 AS DECIMAL(5,2)) AS OrderContributionPercent
FROM customer_orders AS f
INNER JOIN enriched_metrics AS em ON f.CustomerID = em.CustomerID
WHERE em.CustomerActivityStatus = 'Active'


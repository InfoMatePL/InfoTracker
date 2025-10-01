-- dbt model: order_items (source/seed table)
-- In dbt, this would be a seed or source reference
-- For lineage purposes, we represent the schema structure

SELECT
    CAST(NULL AS INT) AS OrderItemID,
    CAST(NULL AS INT) AS OrderID,
    CAST(NULL AS INT) AS ProductID,
    CAST(NULL AS INT) AS Quantity,
    CAST(NULL AS DECIMAL(10,2)) AS UnitPrice,
    CAST(NULL AS DECIMAL(10,2)) AS ExtendedPrice
WHERE 1=0  -- Schema-only representation


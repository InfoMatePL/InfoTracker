-- dbt model: products (source/seed table)
-- In dbt, this would be a seed or source reference
-- For lineage purposes, we represent the schema structure

SELECT
    CAST(NULL AS INT) AS ProductID,
    CAST(NULL AS NVARCHAR(100)) AS ProductName,
    CAST(NULL AS NVARCHAR(50)) AS Category,
    CAST(NULL AS DECIMAL(10,2)) AS Price
WHERE 1=0  -- Schema-only representation


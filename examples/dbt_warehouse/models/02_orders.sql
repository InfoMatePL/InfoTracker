-- dbt model: orders (source/seed table)
-- In dbt, this would be a seed or source reference
-- For lineage purposes, we represent the schema structure

SELECT
    CAST(NULL AS INT) AS OrderID,
    CAST(NULL AS INT) AS CustomerID,
    CAST(NULL AS DATE) AS OrderDate,
    CAST(NULL AS NVARCHAR(50)) AS Status
WHERE 1=0  -- Schema-only representation


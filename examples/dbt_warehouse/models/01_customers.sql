-- dbt model: customers (source/seed table)
-- In dbt, this would be a seed or source reference
-- For lineage purposes, we represent the schema structure

SELECT
    CAST(NULL AS INT) AS CustomerID,
    CAST(NULL AS NVARCHAR(100)) AS CustomerName,
    CAST(NULL AS NVARCHAR(255)) AS Email,
    CAST(NULL AS DATE) AS SignupDate
WHERE 1=0  -- Schema-only representation


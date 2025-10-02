-- dbt model: dim_customer
-- Customer dimension from staging

SELECT
    sc.CustomerID,
    sc.CustomerName,
    sc.EmailDomain,
    sc.SignupDate
FROM DefaultDB.dbo.stg_customers AS sc


CREATE VIEW dbo.dim_customer AS
SELECT
    sc.CustomerID,
    sc.CustomerName,
    sc.EmailDomain,
    sc.SignupDate
FROM dbo.stg_customers AS sc; 
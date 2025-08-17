CREATE VIEW INFOMART.dbo.dim_customer AS
SELECT
    sc.CustomerID,
    sc.CustomerName,
    sc.EmailDomain,
    sc.SignupDate
FROM STG.dbo.stg_customers AS sc; 
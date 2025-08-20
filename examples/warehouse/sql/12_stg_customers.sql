CREATE VIEW STG.dbo.stg_customers AS
SELECT
    c.CustomerID,
    c.CustomerName,
    CASE
        WHEN c.Email IS NOT NULL THEN SUBSTRING(c.Email, CHARINDEX('@', c.Email) + 1, LEN(c.Email))
        ELSE NULL
    END AS EmailDomain,
    c.SignupDate
FROM STG.dbo.Customers AS c; 
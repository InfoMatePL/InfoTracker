-- dbt model: stg_customers
-- Staging transformation on customers source with email domain extraction

SELECT
    c.CustomerID,
    c.CustomerName,
    CASE
        WHEN c.Email IS NOT NULL THEN SUBSTRING(c.Email, CHARINDEX('@', c.Email) + 1, LEN(c.Email))
        ELSE NULL
    END AS EmailDomain,
    c.SignupDate
FROM DefaultDB.dbo.Customers AS c


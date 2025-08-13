CREATE VIEW dbo.stg_customers AS
SELECT
    c.CustomerID,
    c.CustomerName,
    c.Email,
    RIGHT(c.Email, LEN(c.Email) - CHARINDEX('@', c.Email)) AS EmailDomain,
    c.SignupDate
FROM dbo.Customers AS c; 
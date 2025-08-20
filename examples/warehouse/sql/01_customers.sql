CREATE TABLE STG.dbo.Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(255) NULL,
    SignupDate DATE NULL
); 
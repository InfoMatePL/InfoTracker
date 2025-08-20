CREATE TABLE STG.dbo.Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATE NOT NULL,
    Status NVARCHAR(50) NULL
); 
CREATE TABLE STG.dbo.Products (
    ProductID INT PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    Category NVARCHAR(50) NULL,
    UnitPrice DECIMAL(10,2) NOT NULL
); 
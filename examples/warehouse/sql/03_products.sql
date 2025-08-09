CREATE TABLE dbo.Products (
    ProductID INT PRIMARY KEY,
    ProductName NVARCHAR(200) NOT NULL,
    Category NVARCHAR(100) NULL,
    Price DECIMAL(10, 2) NOT NULL
); 
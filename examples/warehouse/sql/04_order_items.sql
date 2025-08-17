CREATE TABLE STG.dbo.OrderItems (
    OrderItemID INT PRIMARY KEY,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    ExtendedPrice AS (Quantity * UnitPrice) PERSISTED
); 
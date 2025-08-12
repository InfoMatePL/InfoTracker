CREATE VIEW dbo.dim_product AS
SELECT
    p.ProductID,
    p.ProductName,
    p.Category,
    p.Price
FROM dbo.Products AS p; 
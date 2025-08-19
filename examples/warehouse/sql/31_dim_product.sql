CREATE VIEW INFOMART.dbo.dim_product AS
SELECT
    p.ProductID,
    p.ProductName,
    p.Category,
    p.UnitPrice
FROM STG.dbo.Products AS p;
-- dbt model: dim_product
-- Product dimension from source

SELECT
    p.ProductID,
    p.ProductName,
    p.Category,
    p.Price
FROM DefaultDB.dbo.Products AS p


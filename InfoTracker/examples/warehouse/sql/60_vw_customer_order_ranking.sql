-- Window Function Examples for Phase 3 Testing
CREATE VIEW dbo.vw_customer_order_ranking AS
SELECT
    o.OrderID,
    o.CustomerID,
    o.OrderDate,
    o.OrderAmount,
    ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS OrderSequence,
    RANK() OVER (ORDER BY o.OrderAmount DESC) AS AmountRank,
    LAG(o.OrderAmount, 1) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS PreviousOrderAmount
FROM dbo.Orders AS o;

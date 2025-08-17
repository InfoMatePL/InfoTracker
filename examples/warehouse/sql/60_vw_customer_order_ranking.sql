-- Window Function Examples for Phase 3 Testing
CREATE VIEW INFOMART.dbo.vw_customer_order_ranking AS
SELECT
    o.OrderID,
    o.CustomerID,
    o.OrderDate,
    o.OrderID AS OrderAmount,
    ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS OrderSequence,
    RANK() OVER (ORDER BY o.OrderID DESC) AS AmountRank,
    LAG(o.OrderID, 1) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS PreviousOrderAmount
FROM STG.dbo.Orders AS o;

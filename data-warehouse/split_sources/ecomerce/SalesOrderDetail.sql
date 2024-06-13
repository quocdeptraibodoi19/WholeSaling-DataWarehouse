USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.SalesOrderDetail;

CREATE TABLE dbo.SalesOrderDetail (
    SalesOrderID INT,
    SalesOrderDetailID INT PRIMARY KEY,
    CarrierTrackingNumber NVARCHAR(25),
    OrderQty INT,
    ProductID INT,
    SpecialOfferID INT,
    UnitPrice MONEY,
    UnitPriceDiscount MONEY,
    LineTotal MONEY,
    ModifiedDate DATETIME,
);

INSERT INTO
    dbo.SalesOrderDetail (
        SalesOrderID,
        [SalesOrderDetailID],
        [CarrierTrackingNumber],
        [OrderQty],
        [ProductID],
        [SpecialOfferID],
        [UnitPrice],
        [UnitPriceDiscount],
        [LineTotal],
        [ModifiedDate]
    )
SELECT
    C.SalesOrderID,
    [SalesOrderDetailID],
    [CarrierTrackingNumber],
    [OrderQty],
    CTE.[ProductID],
    [SpecialOfferID],
    [UnitPrice],
    [UnitPriceDiscount],
    [LineTotal],
    Q.[ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesOrderDetail] Q
    INNER JOIN [AdventureWorks2014].[Production].[Product] CTE ON CTE.ProductID = Q.ProductID
    INNER JOIN [AdventureWorks2014].[Sales].[SalesOrderHeader] C ON C.SalesOrderID = Q.SalesOrderID
WHERE
    C.OnlineOrderFlag = 1
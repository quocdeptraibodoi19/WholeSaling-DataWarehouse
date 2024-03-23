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
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesOrderDetail] Q
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS ProductID,
            [ProductID] AS OldProductID,
            [Name],
            [ProductNumber],
            [MakeFlag],
            [FinishedGoodsFlag],
            [Color],
            [SafetyStockLevel],
            [ReorderPoint],
            [StandardCost],
            [ListPrice],
            [Size],
            [SizeUnitMeasureCode],
            [WeightUnitMeasureCode],
            [Weight],
            [DaysToManufacture],
            [ProductLine],
            [Class],
            [Style],
            [ProductSubcategoryID],
            [ProductModelID],
            [SellStartDate],
            [SellEndDate],
            [DiscontinuedDate],
            [ModifiedDate]
        FROM
            [AdventureWorks2014].[Production].[Product]
    ) AS CTE ON CTE.OldProductID = Q.ProductID
    LEFT JOIN [AdventureWorks2014].[Sales].[SalesOrderHeader] C ON C.SalesOrderID = Q.SalesOrderID
WHERE
    C.OnlineOrderFlag = 1
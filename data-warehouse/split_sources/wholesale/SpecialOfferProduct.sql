USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.SpecialOfferProduct;

CREATE TABLE dbo.SpecialOfferProduct (
    SpecialOfferID INT,
    ProductID INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (SpecialOfferID, ProductID)
);

INSERT INTO
    dbo.SpecialOfferProduct (SpecialOfferID, ProductID, ModifiedDate)
SELECT
    S.SpecialOfferID,
    CTE.ProductID,
    S.ModifiedDate
FROM
    [AdventureWorks2014].[Sales].[SpecialOfferProduct] S
    INNER JOIN (
        SELECT
            [ProductID],
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
            [AdventureWorks2014].[Production].[Product] s
        WHERE
            [ProductID] IN (
                SELECT
                    [ProductID]
                FROM
                    [AdventureWorks2014].[Sales].[SalesOrderDetail] Q
                    INNER JOIN [AdventureWorks2014].[Sales].[SalesOrderHeader] C ON C.SalesOrderID = Q.SalesOrderID
                WHERE
                    C.OnlineOrderFlag = 0
            )
    ) AS CTE ON CTE.ProductID = S.ProductID
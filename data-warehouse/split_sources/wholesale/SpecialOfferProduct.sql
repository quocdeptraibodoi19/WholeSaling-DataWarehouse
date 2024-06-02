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
    ) AS CTE ON CTE.OldProductID = S.ProductID
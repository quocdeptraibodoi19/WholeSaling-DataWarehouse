USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductProductPhoto;

CREATE TABLE ProductProductPhoto (
    ProductID INT,
    ProductPhotoID INT,
    [Primary] BIT,
    ModifiedDate DATETIME,
    PRIMARY KEY (ProductID, ProductPhotoID)
);

INSERT INTO
    ProductProductPhoto (
        ProductID,
        ProductPhotoID,
        [Primary],
        ModifiedDate
    )
SELECT
    CTE.newProductID AS ProductID,
    [ProductPhotoID],
    [Primary],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductProductPhoto] S
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS newProductID,
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
            [AdventureWorks2014].[Production].[Product]
    ) AS CTE ON CTE.ProductID = S.ProductID
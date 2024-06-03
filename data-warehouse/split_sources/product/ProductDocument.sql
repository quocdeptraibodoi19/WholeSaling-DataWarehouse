USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductDocument;

CREATE TABLE dbo.ProductDocument (
    ProductID INT,
    DocumentNode NVARCHAR(255),
    ModifiedDate DATETIME,
    PRIMARY KEY (ProductID, DocumentNode)
);

INSERT INTO
    dbo.ProductDocument (ProductID, DocumentNode, ModifiedDate)
SELECT
    CTE.newProductID AS ProductID,
    CONVERT(NVARCHAR(255), [DocumentNode]) AS DocumentNode,
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductDocument] S
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
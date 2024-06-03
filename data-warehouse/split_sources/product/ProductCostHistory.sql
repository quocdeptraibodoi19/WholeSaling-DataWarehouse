USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductCostHistory;

CREATE TABLE dbo.ProductCostHistory (
    ProductID INT,
    StartDate DATE,
    EndDate DATE,
    StandardCost DECIMAL(18, 2),
    ModifiedDate DATETIME,
    PRIMARY KEY (ProductID, StartDate)
);

INSERT INTO
    dbo.ProductCostHistory (
        ProductID,
        StartDate,
        EndDate,
        StandardCost,
        ModifiedDate
    )
SELECT
    CTE.newProductID AS ProductID,
    [StartDate],
    [EndDate],
    S.[StandardCost],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductCostHistory] S
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
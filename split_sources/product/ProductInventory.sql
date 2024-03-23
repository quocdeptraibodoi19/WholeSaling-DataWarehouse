USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductInventory;

CREATE TABLE dbo.ProductInventory (
    ProductID INT,
    LocationID INT,
    Shelf NVARCHAR(50),
    Bin NVARCHAR(50),
    Quantity INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (ProductID, LocationID)
);

INSERT INTO
    dbo.ProductInventory (
        ProductID,
        LocationID,
        Shelf,
        Bin,
        Quantity,
        ModifiedDate
    )
SELECT
    CTE.newProductID AS ProductID,
    [LocationID],
    [Shelf],
    [Bin],
    [Quantity],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductInventory] S
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
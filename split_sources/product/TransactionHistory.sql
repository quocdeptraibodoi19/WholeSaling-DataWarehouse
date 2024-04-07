USE Product;

GO
    DROP TABLE IF EXISTS dbo.TransactionHistory;

CREATE TABLE dbo.TransactionHistory (
    TransactionID INT PRIMARY KEY,
    ProductID INT,
    ReferenceOrderID INT,
    ReferenceOrderLineID INT,
    TransactionDate DATETIME,
    TransactionType NVARCHAR(1),
    Quantity INT,
    ActualCost MONEY,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.TransactionHistory (
        TransactionID,
        ProductID,
        ReferenceOrderID,
        ReferenceOrderLineID,
        TransactionDate,
        TransactionType,
        Quantity,
        ActualCost,
        ModifiedDate
    )
SELECT
    S.[TransactionID],
    CTE.[ProductID],
    S.[ReferenceOrderID],
    S.[ReferenceOrderLineID],
    S.[TransactionDate],
    S.[TransactionType],
    S.[Quantity],
    S.[ActualCost],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[TransactionHistory] S
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
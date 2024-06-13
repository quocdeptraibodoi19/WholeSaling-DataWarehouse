USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.Product;

CREATE TABLE dbo.Product (
    ProductID INT PRIMARY KEY,
    [Name] NVARCHAR(255),
    [ProductNumber] NVARCHAR(25),
    [MakeFlag] BIT,
    [FinishedGoodsFlag] BIT,
    [Color] NVARCHAR(15),
    [SafetyStockLevel] SMALLINT,
    [ReorderPoint] SMALLINT,
    [StandardCost] DECIMAL(18, 2),
    [ListPrice] DECIMAL(18, 2),
    [Size] NVARCHAR(5),
    [SizeUnitMeasureCode] NVARCHAR(3),
    [WeightUnitMeasureCode] NVARCHAR(3),
    [Weight] DECIMAL(18, 2),
    [DaysToManufacture] INT,
    [ProductLine] NCHAR(2),
    [Class] NCHAR(2),
    [Style] NCHAR(2),
    [ProductSubcategoryID] INT,
    [ProductModelID] INT,
    [SellStartDate] DATETIME,
    [SellEndDate] DATETIME,
    [DiscontinuedDate] DATETIME,
    [ModifiedDate] DATETIME
);

INSERT INTO
    dbo.Product (
        ProductID,
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
    )
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
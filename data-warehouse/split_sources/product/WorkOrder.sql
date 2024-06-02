USE Product;

GO
    DROP TABLE IF EXISTS dbo.WorkOrder;

CREATE TABLE dbo.WorkOrder (
    WorkOrderID INT PRIMARY KEY,
    ProductID INT,
    OrderQty INT,
    StockedQty INT,
    ScrappedQty INT,
    StartDate DATE,
    EndDate DATE,
    DueDate DATE,
    ScrapReasonID INT,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.WorkOrder (
        WorkOrderID,
        ProductID,
        OrderQty,
        StockedQty,
        ScrappedQty,
        StartDate,
        EndDate,
        DueDate,
        ScrapReasonID,
        ModifiedDate
    )
SELECT
    [WorkOrderID],
    CTE.newProductID AS ProductID,
    [OrderQty],
    [StockedQty],
    [ScrappedQty],
    [StartDate],
    [EndDate],
    [DueDate],
    [ScrapReasonID],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[WorkOrder] S
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
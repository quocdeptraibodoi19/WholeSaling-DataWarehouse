USE Product;

GO
    DROP TABLE IF EXISTS dbo.WorkOrderRouting;

CREATE TABLE dbo.WorkOrderRouting (
    WorkOrderID INT,
    ProductID INT,
    OperationSequence INT,
    LocationID INT,
    ScheduledStartDate DATETIME,
    ScheduledEndDate DATETIME,
    ActualStartDate DATETIME,
    ActualEndDate DATETIME,
    ActualResourceHrs DECIMAL(18, 2),
    PlannedCost DECIMAL(18, 2),
    ActualCost DECIMAL(18, 2),
    ModifiedDate DATETIME,
    PRIMARY KEY (WorkOrderID, ProductID, OperationSequence)
);

INSERT INTO
    dbo.WorkOrderRouting (
        [WorkOrderID],
        [ProductID],
        [OperationSequence],
        [LocationID],
        [ScheduledStartDate],
        [ScheduledEndDate],
        [ActualStartDate],
        [ActualEndDate],
        [ActualResourceHrs],
        [PlannedCost],
        [ActualCost],
        [ModifiedDate]
    )
SELECT
    [WorkOrderID],
    CTE.newProductID AS ProductID,
    [OperationSequence],
    [LocationID],
    [ScheduledStartDate],
    [ScheduledEndDate],
    [ActualStartDate],
    [ActualEndDate],
    [ActualResourceHrs],
    [PlannedCost],
    [ActualCost],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[WorkOrderRouting] S
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
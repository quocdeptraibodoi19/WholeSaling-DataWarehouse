USE Product;

GO
    DROP TABLE IF EXISTS dbo.PurchaseOrderDetail;

CREATE TABLE dbo.PurchaseOrderDetail (
    PurchaseOrderID INT,
    PurchaseOrderDetailID INT,
    DueDate DATETIME,
    OrderQty INT,
    ProductID INT,
    UnitPrice MONEY,
    LineTotal MONEY,
    ReceivedQty INT,
    RejectedQty INT,
    StockedQty INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (PurchaseOrderID, PurchaseOrderDetailID),
);

INSERT INTO
    dbo.PurchaseOrderDetail (
        [PurchaseOrderID],
        [PurchaseOrderDetailID],
        [DueDate],
        [OrderQty],
        ProductID,
        [UnitPrice],
        [LineTotal],
        [ReceivedQty],
        [RejectedQty],
        [StockedQty],
        [ModifiedDate]
    )
SELECT
    [PurchaseOrderID],
    [PurchaseOrderDetailID],
    [DueDate],
    [OrderQty],
    CTE.newProductID AS ProductID,
    [UnitPrice],
    [LineTotal],
    [ReceivedQty],
    [RejectedQty],
    [StockedQty],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Purchasing].[PurchaseOrderDetail] S
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
USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductVendor;

CREATE TABLE dbo.ProductVendor (
    ProductID INT,
    VendorID INT,
    AverageLeadTime INT,
    StandardPrice MONEY,
    LastReceiptCost MONEY,
    LastReceiptDate DATE,
    MinOrderQty INT,
    MaxOrderQty INT,
    OnOrderQty INT,
    UnitMeasureCode NVARCHAR(3),
    ModifiedDate DATETIME,
    PRIMARY KEY (ProductID, VendorID),
);

INSERT INTO
    dbo.ProductVendor (
        ProductID,
        VendorID,
        AverageLeadTime,
        StandardPrice,
        LastReceiptCost,
        LastReceiptDate,
        MinOrderQty,
        MaxOrderQty,
        OnOrderQty,
        UnitMeasureCode,
        ModifiedDate
    )
SELECT
    CTE1.newProductID AS ProductID,
    CTE.VendorID,
    [AverageLeadTime],
    [StandardPrice],
    [LastReceiptCost],
    [LastReceiptDate],
    [MinOrderQty],
    [MaxOrderQty],
    [OnOrderQty],
    [UnitMeasureCode],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Purchasing].[ProductVendor] S
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS VendorID,
            [BusinessEntityID],
            [AccountNumber],
            [Name],
            [CreditRating],
            [PreferredVendorStatus],
            [ActiveFlag],
            [PurchasingWebServiceURL],
            [ModifiedDate]
        FROM
            [AdventureWorks2014].[Purchasing].[Vendor]
    ) AS CTE ON CTE.BusinessEntityID = S.BusinessEntityID
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
    ) AS CTE1 ON CTE1.ProductID = S.ProductID
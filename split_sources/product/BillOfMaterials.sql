USE Product;

GO
    DROP TABLE IF EXISTS dbo.BillOfMaterials;

CREATE TABLE dbo.BillOfMaterials (
    BillOfMaterialsID INT PRIMARY KEY,
    ProductAssemblyID INT,
    ComponentID INT,
    StartDate DATETIME,
    EndDate DATETIME,
    UnitMeasureCode NVARCHAR(3),
    BOMLevel INT,
    PerAssemblyQty DECIMAL(18, 2),
    ModifiedDate DATETIME,
    FOREIGN KEY (ProductAssemblyID) REFERENCES dbo.Product(ProductID)
);

INSERT INTO
    dbo.BillOfMaterials (
        [BillOfMaterialsID],
        [ProductAssemblyID],
        [ComponentID],
        [StartDate],
        [EndDate],
        [UnitMeasureCode],
        [BOMLevel],
        [PerAssemblyQty],
        [ModifiedDate]
    )
SELECT
    [BillOfMaterialsID],
    CTE.newProductID AS [ProductAssemblyID],
    [ComponentID],
    [StartDate],
    [EndDate],
    [UnitMeasureCode],
    [BOMLevel],
    [PerAssemblyQty],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[BillOfMaterials] S
    LEFT JOIN (
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
    ) AS CTE ON CTE.ProductID = S.ProductAssemblyID
USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductSubcategory;

CREATE TABLE dbo.ProductSubcategory (
    ProductSubcategoryID INT,
    ProductCategoryID INT,
    [Name] NVARCHAR(50),
    ModifiedDate DATETIME,
    PRIMARY KEY(ProductSubcategoryID, ProductCategoryID)
);

INSERT INTO
    dbo.ProductSubcategory (
        ProductSubcategoryID,
        ProductCategoryID,
        Name,
        ModifiedDate
    )
SELECT
    [ProductSubcategoryID],
    [ProductCategoryID],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductSubcategory]
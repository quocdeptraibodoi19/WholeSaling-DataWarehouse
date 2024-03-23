USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductModel;

CREATE TABLE dbo.ProductModel (
    ProductModelID INT PRIMARY KEY,
    Name NVARCHAR(255),
    CatalogDescription XML,
    Instructions XML,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ProductModel (
        ProductModelID,
        Name,
        CatalogDescription,
        Instructions,
        ModifiedDate
    )
SELECT
    [ProductModelID],
    [Name],
    CONVERT(NVARCHAR(MAX), [CatalogDescription]) AS CatalogDescription,
    CONVERT(NVARCHAR(MAX), [Instructions]) AS Instructions,
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductModel];
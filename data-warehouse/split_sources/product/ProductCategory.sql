USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductCategory;

CREATE TABLE dbo.ProductCategory (
    ProductCategoryID INT PRIMARY KEY,
    [Name] NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ProductCategory (ProductCategoryID, Name, ModifiedDate)
SELECT
    [ProductCategoryID],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductCategory]
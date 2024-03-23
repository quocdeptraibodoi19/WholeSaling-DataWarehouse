USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductDescription;

CREATE TABLE dbo.ProductDescription (
    ProductDescriptionID INT PRIMARY KEY,
    Description NVARCHAR(MAX),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ProductDescription (ProductDescriptionID, Description, ModifiedDate)
SELECT
    [ProductDescriptionID],
    [Description],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductDescription]
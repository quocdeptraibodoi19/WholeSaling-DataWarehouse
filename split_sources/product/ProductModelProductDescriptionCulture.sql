USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductModelProductDescriptionCulture;

CREATE TABLE dbo.ProductModelProductDescriptionCulture (
    ProductModelID INT,
    ProductDescriptionID INT,
    CultureID NVARCHAR(10),
    ModifiedDate DATETIME,
    PRIMARY KEY (ProductModelID, ProductDescriptionID, CultureID)
);

INSERT INTO
    ProductModelProductDescriptionCulture (
        ProductModelID,
        ProductDescriptionID,
        CultureID,
        ModifiedDate
    )
SELECT
    [ProductModelID],
    [ProductDescriptionID],
    [CultureID],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductModelProductDescriptionCulture]
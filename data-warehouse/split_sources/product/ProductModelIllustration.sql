USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductModelIllustration;

CREATE TABLE dbo.ProductModelIllustration (
    ProductModelID INT,
    IllustrationID INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (ProductModelID, IllustrationID)
);

INSERT INTO
    dbo.ProductModelIllustration (ProductModelID, IllustrationID, ModifiedDate)
SELECT
    [ProductModelID],
    [IllustrationID],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductModelIllustration]
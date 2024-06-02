USE Product;

GO
    DROP TABLE IF EXISTS dbo.ScrapReason;

CREATE TABLE dbo.ScrapReason (
    ScrapReasonID INT PRIMARY KEY,
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ScrapReason (
        [ScrapReasonID],
        [Name],
        [ModifiedDate]
    )
SELECT
    [ScrapReasonID],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ScrapReason]
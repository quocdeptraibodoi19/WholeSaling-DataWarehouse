USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.SalesReason;

CREATE TABLE dbo.SalesReason (
    SalesReasonID INT PRIMARY KEY,
    Name NVARCHAR(255),
    ReasonType NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.SalesReason (
        [SalesReasonID],
        [Name],
        [ReasonType],
        [ModifiedDate]
    )
SELECT
    [SalesReasonID],
    [Name],
    [ReasonType],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesReason]
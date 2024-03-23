USE Product;

GO
    DROP TABLE IF EXISTS dbo.UnitMeasure;

CREATE TABLE dbo.UnitMeasure (
    UnitMeasureCode NVARCHAR(3) PRIMARY KEY,
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.UnitMeasure ([UnitMeasureCode], [Name], [ModifiedDate])
SELECT
    [UnitMeasureCode],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[UnitMeasure]
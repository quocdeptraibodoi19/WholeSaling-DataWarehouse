USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.CountryRegion;

CREATE TABLE dbo.CountryRegion (
    CountryRegionCode NVARCHAR(3) PRIMARY KEY,
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.CountryRegion ([CountryRegionCode], [Name], [ModifiedDate])
SELECT
    [CountryRegionCode],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Person].[CountryRegion]
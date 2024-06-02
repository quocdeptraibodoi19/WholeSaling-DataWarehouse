USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.CountryRegion;

CREATE TABLE dbo.CountryRegion (
    CountryRegionCode NVARCHAR(2) PRIMARY KEY,
    CountryRegionName NVARCHAR(255),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.CountryRegion (
        CountryRegionCode,
        CountryRegionName,
        ModifiedDate
    )
SELECT
    [CountryRegionCode],
    [Name] AS CountryRegionName,
    ModifiedDate
FROM
    [AdventureWorks2014].[Person].[CountryRegion]
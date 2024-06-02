USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.StateProvince;

CREATE TABLE dbo.StateProvince (
    StateProvinceID INT PRIMARY KEY,
    StateProvinceCode NVARCHAR(5),
    CountryRegionCode NVARCHAR(3),
    IsOnlyStateProvinceFlag BIT,
    Name NVARCHAR(50),
    TerritoryID INT,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.StateProvince(
        [StateProvinceID],
        [StateProvinceCode],
        [CountryRegionCode],
        [IsOnlyStateProvinceFlag],
        [Name],
        [TerritoryID],
        [ModifiedDate]
    )
SELECT
    [StateProvinceID],
    [StateProvinceCode],
    [CountryRegionCode],
    [IsOnlyStateProvinceFlag],
    [Name],
    [TerritoryID],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Person].[StateProvince]
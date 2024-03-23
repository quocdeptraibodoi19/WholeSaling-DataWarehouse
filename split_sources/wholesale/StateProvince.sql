USE WholeSaling;

GO
    CREATE TABLE dbo.StateProvince (
        StateProvinceID INT PRIMARY KEY,
        StateProvinceCode NVARCHAR(255),
        CountryRegionCode NVARCHAR(2),
        IsOnlyStateProvinceFlag BIT,
        Name NVARCHAR(255),
        TerritoryID INT,
        ModifiedDate DATETIME
    );

INSERT INTO
    dbo.StateProvince (
        StateProvinceID,
        StateProvinceCode,
        CountryRegionCode,
        IsOnlyStateProvinceFlag,
        Name,
        TerritoryID,
        ModifiedDate
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
    [AdventureWorks2014].[Person].[StateProvince];
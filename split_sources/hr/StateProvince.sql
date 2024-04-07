USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.StateProvince;

CREATE TABLE dbo.StateProvince (
    StateProvinceID INT PRIMARY KEY,
    StateProvinceCode NVARCHAR(255),
    CountryRegionCode NVARCHAR(3),
    IsOnlyStateProvinceFlag BIT,
    StateProvinceName NVARCHAR(255),
    TerritoryID INT,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.StateProvince (
        StateProvinceID,
        StateProvinceCode,
        CountryRegionCode,
        IsOnlyStateProvinceFlag,
        StateProvinceName,
        TerritoryID,
        ModifiedDate
    )
SELECT
    [StateProvinceID],
    [StateProvinceCode],
    T.CountryCode AS CountryRegionCode,
    [IsOnlyStateProvinceFlag],
    S.Name as StateProvinceName,
    [TerritoryID],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Person].[StateProvince] S
    LEFT JOIN [HumanResourceSystem].[dbo].[CountryRegion] T ON S.CountryRegionCode = T.Alpha2Code
USE Product;

GO
    DROP TABLE IF EXISTS dbo.StateProvinceCountry;

CREATE TABLE dbo.StateProvinceCountry (
    StateProvinceID INT PRIMARY KEY,
    StateProvinceCode NVARCHAR(3),
    CountryName NVARCHAR(50),
    IsOnlyStateProvinceFlag BIT,
    StateProviceName NVARCHAR(50),
    TerritoryID INT,
    ModifiedDate DATETIME,
);

INSERT INTO
    dbo.StateProvinceCountry (
        StateProvinceID,
        StateProvinceCode,
        CountryName,
        IsOnlyStateProvinceFlag,
        StateProviceName,
        TerritoryID,
        ModifiedDate
    )
SELECT
    [StateProvinceID],
    [StateProvinceCode],
    T.Name AS CountryName,
    [IsOnlyStateProvinceFlag],
    S.Name AS StateProviceName,
    [TerritoryID],
CASE
        WHEN S.ModifiedDate > T.ModifiedDate THEN S.ModifiedDate
        ELSE T.ModifiedDate
    END AS ModifiedDate
FROM
    [AdventureWorks2014].[Person].[StateProvince] S
    INNER JOIN [AdventureWorks2014].[Person].[CountryRegion] T ON S.CountryRegionCode = T.CountryRegionCode;
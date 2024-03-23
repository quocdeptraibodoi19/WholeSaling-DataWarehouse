USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.CountryRegionCurrency;

CREATE TABLE dbo.CountryRegionCurrency (
    CountryRegionCode NVARCHAR(3),
    CurrencyCode NVARCHAR(3),
    ModifiedDate DATETIME,
    PRIMARY KEY (CountryRegionCode, CurrencyCode)
)
INSERT INTO
    dbo.CountryRegionCurrency (
        CountryRegionCode,
        CurrencyCode,
        ModifiedDate
    )
SELECT
    [CountryRegionCode],
    [CurrencyCode],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[CountryRegionCurrency]
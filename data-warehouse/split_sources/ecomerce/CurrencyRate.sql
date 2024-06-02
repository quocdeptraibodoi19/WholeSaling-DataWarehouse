USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.CurrencyRate;

CREATE TABLE dbo.CurrencyRate (
    CurrencyRateID INT PRIMARY KEY,
    CurrencyRateDate DATETIME,
    FromCurrencyCode NVARCHAR(3),
    ToCurrencyCode NVARCHAR(3),
    AverageRate MONEY,
    EndOfDayRate MONEY,
    ModifiedDate DATETIME
);

INSERT INTO dbo.CurrencyRate (
    [CurrencyRateID],
    [CurrencyRateDate],
    [FromCurrencyCode],
    [ToCurrencyCode],
    [AverageRate],
    [EndOfDayRate],
    [ModifiedDate]
)
SELECT
    [CurrencyRateID],
    [CurrencyRateDate],
    [FromCurrencyCode],
    [ToCurrencyCode],
    [AverageRate],
    [EndOfDayRate],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[CurrencyRate]
USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.Currency;

CREATE TABLE dbo.Currency (
    CurrencyCode NVARCHAR(3) PRIMARY KEY,
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Currency ([CurrencyCode], [Name], [ModifiedDate])
SELECT
    [CurrencyCode],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[Currency]
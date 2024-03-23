USE WholeSaling;

GO
    CREATE TABLE IF NOT EXISTS dbo.Currency (
        CurrencyCode NVARCHAR(3) PRIMARY KEY,
        Name NVARCHAR(50),
        ModifiedDate DATETIME
    );

NSERT INTO dbo.Currency ([CurrencyCode], [Name], [ModifiedDate])
SELECT
    [CurrencyCode],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[Currency]
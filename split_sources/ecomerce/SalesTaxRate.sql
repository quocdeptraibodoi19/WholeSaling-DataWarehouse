USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.SalesTaxRate;

CREATE TABLE dbo.SalesTaxRate (
    SalesTaxRateID INT PRIMARY KEY,
    StateProvinceID INT,
    TaxType NVARCHAR(10),
    TaxRate DECIMAL(8, 4),
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.SalesTaxRate (
        [SalesTaxRateID],
        [StateProvinceID],
        [TaxType],
        [TaxRate],
        [Name],
        [ModifiedDate]
    )
SELECT
    [SalesTaxRateID],
    [StateProvinceID],
    [TaxType],
    [TaxRate],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesTaxRate]
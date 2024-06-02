USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.ShipMethod;

CREATE TABLE dbo.ShipMethod (
    ShipMethodID INT PRIMARY KEY,
    Name NVARCHAR(50),
    ShipBase DECIMAL(10, 2),
    ShipRate DECIMAL(10, 2),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ShipMethod (
        [ShipMethodID],
        [Name],
        [ShipBase],
        [ShipRate],
        [ModifiedDate]
    )
SELECT
    [ShipMethodID],
    [Name],
    [ShipBase],
    [ShipRate],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Purchasing].[ShipMethod]
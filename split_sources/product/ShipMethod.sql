USE Product;

GO
    DROP TABLE IF EXISTS dbo.ShipMethod;

CREATE TABLE dbo.ShipMethod (
    ShipMethodID INT PRIMARY KEY,
    Name NVARCHAR(50),
    ShipBase MONEY,
    ShipRate MONEY,
    rowguid UNIQUEIDENTIFIER,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ShipMethod (
        [ShipMethodID],
        [Name],
        [ShipBase],
        [ShipRate],
        [rowguid],
        [ModifiedDate]
    )
SELECT
    [ShipMethodID],
    [Name],
    [ShipBase],
    [ShipRate],
    [rowguid],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Purchasing].[ShipMethod]
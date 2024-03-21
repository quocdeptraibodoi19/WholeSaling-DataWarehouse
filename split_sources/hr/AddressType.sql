USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.AddressType;

CREATE TABLE dbo.AddressType (
    AddressTypeID INT PRIMARY KEY,
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.AddressType (AddressTypeID, Name, ModifiedDate)
SELECT
    [AddressTypeID],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Person].[AddressType]
USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.PhoneNumberType;

CREATE TABLE dbo.PhoneNumberType (
    PhoneNumberTypeID INT PRIMARY KEY,
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.PhoneNumberType (PhoneNumberTypeID, Name, ModifiedDate)
SELECT
    PhoneNumberTypeID,
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Person].[PhoneNumberType]
Use WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.ContactType;

CREATE TABLE dbo.ContactType (
    ContactTypeID INT PRIMARY KEY,
    Name NVARCHAR(255),
    ModifiedDate DATETIME
);

INSERT INTO
    [dbo].[ContactType] (ContactTypeID, Name, ModifiedDate)
SELECT
    ContactTypeID,
    Name,
    ModifiedDate
FROM
    [AdventureWorks2014].[Person].[ContactType];
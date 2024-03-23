Use HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.StackholderPosition;

CREATE TABLE dbo.StackholderPosition (
    PositionTypeID INT PRIMARY KEY,
    PositionName NVARCHAR(255),
    ModifiedDate DATETIME
);

INSERT INTO
    [dbo].[StackholderPosition] (PositionTypeID, PositionName, ModifiedDate)
SELECT
    ContactTypeID as PositionTypeID,
    Name as PositionName,
    ModifiedDate
FROM
    [AdventureWorks2014].[Person].[ContactType];
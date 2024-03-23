USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.Shift;

CREATE TABLE dbo.Shift (
    ShiftID INT PRIMARY KEY,
    Name NVARCHAR(50),
    StartTime TIME,
    EndTime TIME,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Shift (ShiftID, Name, StartTime, EndTime, ModifiedDate)
SELECT
    [ShiftID],
    [Name],
    [StartTime],
    [EndTime],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[HumanResources].[Shift]
USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.Department;

CREATE TABLE dbo.Department (
    DepartmentID INT PRIMARY KEY,
    Name NVARCHAR(50),
    GroupName NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Department (DepartmentID, Name, GroupName, ModifiedDate)
SELECT
    DepartmentID,
    Name,
    GroupName,
    ModifiedDate
FROM
    [AdventureWorks2014].[HumanResources].[Department]
USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.Employee;

CREATE TABLE dbo.Employee (
    EmployeeID INT PRIMARY KEY,
    NationalIDNumber NVARCHAR(15),
    LoginID NVARCHAR(256),
    OrganizationNode HIERARCHYID,
    OrganizationLevel INT,
    Jobtitle NVARCHAR(50),
    BirthDate DATE,
    MaritalStatus NVARCHAR(1),
    Gender NVARCHAR(1),
    HireDate DATE,
    SalariedFlag BIT,
    VacationHours INT,
    SickLeaveHours INT,
    CurrentFlag BIT,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Employee (
        EmployeeID,
        NationalIDNumber,
        LoginID,
        OrganizationNode,
        OrganizationLevel,
        Jobtitle,
        BirthDate,
        MaritalStatus,
        Gender,
        HireDate,
        SalariedFlag,
        VacationHours,
        SickLeaveHours,
        CurrentFlag,
        ModifiedDate
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS EmployeeID,
    NationalIDNumber,
    LoginID,
    OrganizationNode,
    OrganizationLevel,
    Jobtitle,
    BirthDate,
    MaritalStatus,
    Gender,
    HireDate,
    SalariedFlag,
    VacationHours,
    SickLeaveHours,
    CurrentFlag,
    ModifiedDate
FROM
    [AdventureWorks2014].[HumanResources].[Employee];
/* Employee consists of EM and SP */
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
    PersonType NVARCHAR(2),
    NameStyle BIT,
    Title NVARCHAR(8),
    FirstName NVARCHAR(50),
    MiddleName NVARCHAR(50),
    LastName NVARCHAR(50),
    Suffix NVARCHAR(10),
    EmailPromotion INT,
    AdditionalContactInfo XML,
    Demographics XML,
    PasswordHash NVARCHAR(100),
    PasswordSalt NVARCHAR(50),
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
        PersonType,
        NameStyle,
        Title,
        FirstName,
        MiddleName,
        LastName,
        Suffix,
        EmailPromotion,
        AdditionalContactInfo,
        Demographics,
        PasswordHash,
        PasswordSalt,
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
    S.NationalIDNumber,
    S.LoginID,
    S.OrganizationNode,
    S.OrganizationLevel,
    S.Jobtitle,
    S.BirthDate,
    S.MaritalStatus,
    S.Gender,
    S.HireDate,
    S.SalariedFlag,
    S.VacationHours,
    S.SickLeaveHours,
    S.CurrentFlag,
    T.PersonType,
    T.NameStyle,
    T.Title,
    T.FirstName,
    T.MiddleName,
    T.LastName,
    T.Suffix,
    T.EmailPromotion,
    T.AdditionalContactInfo,
    T.Demographics,
    K.PasswordHash,
    K.PasswordSalt,
    S.ModifiedDate
FROM
    [AdventureWorks2014].[HumanResources].[Employee] S
INNER JOIN [AdventureWorks2014].[Person].[Person] T
ON S.BusinessEntityID = T.BusinessEntityID
INNER JOIN [AdventureWorks2014].[Person].[Password] K
ON S.BusinessEntityID = K.BusinessEntityID
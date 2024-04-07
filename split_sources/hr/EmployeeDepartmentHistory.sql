USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.EmployeeDepartmentHistory;

CREATE TABLE dbo.EmployeeDepartmentHistory (
    EmployeeID INT,
    DepartmentID INT,
    ShiftID INT,
    StartDate DATE,
    EndDate DATE,
    ModifiedDate DATETIME,
    PRIMARY KEY (EmployeeID, DepartmentID, ShiftID, StartDate) -- Modify the primary key as needed
);

INSERT INTO
    dbo.EmployeeDepartmentHistory (
        EmployeeID,
        DepartmentID,
        ShiftID,
        StartDate,
        EndDate,
        ModifiedDate
    )
SELECT
    CTE.EmployeeID,
    T.DepartmentID,
    T.ShiftID,
    T.StartDate,
    T.EndDate,
    T.ModifiedDate
FROM
    [AdventureWorks2014].[HumanResources].[EmployeeDepartmentHistory] AS T
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS EmployeeID,
            S.BusinessEntityID,
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
            INNER JOIN [AdventureWorks2014].[Person].[Person] T ON S.BusinessEntityID = T.BusinessEntityID
            INNER JOIN [AdventureWorks2014].[Person].[Password] K ON S.BusinessEntityID = K.BusinessEntityID
    ) AS CTE ON CTE.BusinessEntityID = T.BusinessEntityID;
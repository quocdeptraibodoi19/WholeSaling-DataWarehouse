USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.JobCandidate;

CREATE TABLE dbo.JobCandidate (
    JobCandidateID INT PRIMARY KEY,
    EmployeeID INT,
    Resume NVARCHAR(MAX),
    -- Adjust the data type based on your needs
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.JobCandidate (JobCandidateID, EmployeeID, Resume, ModifiedDate)
SELECT
    T.JobCandidateID,
    CTE.EmployeeID,
    CONVERT(NVARCHAR(MAX), T.[Resume]) AS Resume,
    T.ModifiedDate
FROM
    [AdventureWorks2014].[HumanResources].[JobCandidate] AS T
    LEFT JOIN (
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
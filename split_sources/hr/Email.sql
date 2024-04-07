USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.Email;

CREATE TABLE dbo.Email (
    ID INT,
    [Source] NVARCHAR(12),
    EmailAddressID INT,
    EmailAddress NVARCHAR(255),
    ModifiedDate DATETIME,
    PRIMARY KEY (ID, [Source])
);

INSERT INTO
    dbo.Email (
        ID,
        [Source],
        EmailAddressID,
        EmailAddress,
        ModifiedDate
    )
SELECT
    CTE.StackHolderID AS ID,
    'StakeHolder' AS Source,
    [EmailAddressID],
    [EmailAddress],
    K.ModifiedDate
FROM
    [AdventureWorks2014].[Person].[EmailAddress] K
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS StackHolderID,
            PersonType,
            T.BusinessEntityID,
            NameStyle,
            Title,
            FirstName,
            MiddleName,
            LastName,
            Suffix,
            EmailPromotion,
            AdditionalContactInfo,
            Demographics,
            ModifiedDate
        FROM
            [AdventureWorks2014].[Person].[Person] T
        WHERE
            PersonType IN ('VC', 'GC', 'SC')
    ) AS CTE ON CTE.BusinessEntityID = K.BusinessEntityID
UNION
ALL
SELECT
    CTE.EmployeeID AS ID,
    'Employee' AS Source,
    [EmailAddressID],
    [EmailAddress],
    K.ModifiedDate
FROM
    [AdventureWorks2014].[Person].[EmailAddress] K
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
    ) AS CTE ON CTE.BusinessEntityID = K.BusinessEntityID
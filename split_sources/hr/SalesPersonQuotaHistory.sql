USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.SalesPersonQuotaHistory;

CREATE TABLE dbo.SalesPersonQuotaHistory (
    EmployeeID INT,
    QuotaDate DATE,
    SalesQuota DECIMAL(18, 2),
    ModifiedDate DATETIME,
    PRIMARY KEY (EmployeeID, QuotaDate)
);

INSERT INTO
    dbo.SalesPersonQuotaHistory (EmployeeID, QuotaDate, SalesQuota, ModifiedDate)
SELECT
    CTE.EmployeeID,
    [QuotaDate],
    [SalesQuota],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesPersonQuotaHistory] S
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
    ) AS CTE ON CTE.BusinessEntityID = S.BusinessEntityID
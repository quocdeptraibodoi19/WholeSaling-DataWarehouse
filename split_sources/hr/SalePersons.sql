USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.SalePersons;

CREATE TABLE dbo.SalePersons (
    EmployeeID INT,
    TerritoryID INT,
    SalesQuota DECIMAL(18, 2),
    Bonus DECIMAL(18, 2),
    CommissionPct DECIMAL(5, 2),
    SalesYTD DECIMAL(18, 2),
    SalesLastYear DECIMAL(18, 2),
    ModifiedDate DATETIME,
    PRIMARY KEY (EmployeeID)
);

INSERT INTO
    dbo.SalePersons (
        EmployeeID,
        TerritoryID,
        SalesQuota,
        Bonus,
        CommissionPct,
        SalesYTD,
        SalesLastYear,
        ModifiedDate
    )
SELECT
    CTE.EmployeeID,
    [TerritoryID],
    [SalesQuota],
    [Bonus],
    [CommissionPct],
    [SalesYTD],
    [SalesLastYear],
    S.ModifiedDate
FROM
    [AdventureWorks2014].[Sales].[SalesPerson] S
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
    ) AS CTE ON CTE.BusinessEntityID = S.BusinessEntityID
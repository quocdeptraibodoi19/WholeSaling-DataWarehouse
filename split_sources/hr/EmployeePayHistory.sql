USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.EmployeePayHistory;

CREATE TABLE dbo.EmployeePayHistory (
    EmployeeID INT,
    RateChangeDate DATE,
    Rate DECIMAL(18, 2),
    -- Adjust the precision and scale based on your needs
    PayFrequency NVARCHAR(50),
    -- Adjust the length based on your needs
    ModifiedDate DATETIME,
    PRIMARY KEY (EmployeeID, RateChangeDate) -- Modify the primary key as needed
);

INSERT INTO
    dbo.EmployeePayHistory (
        EmployeeID,
        RateChangeDate,
        Rate,
        PayFrequency,
        ModifiedDate
    )
SELECT
    CTE.EmployeeID,
    T.RateChangeDate,
    T.Rate,
    T.PayFrequency,
    T.ModifiedDate
FROM
    [AdventureWorks2014].[HumanResources].[EmployeePayHistory] AS T
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
    ) AS CTE ON CTE.BusinessEntityID = T.BusinessEntityID;
USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.EmployeeAddress;

CREATE TABLE dbo.EmployeeAddress (
    EmployeeID INT,
    AddressID INT,
    AddressTypeID INT,
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(50),
    StateProvinceID INT,
    PostalCode NVARCHAR(15),
    SpatialLocation GEOGRAPHY,
    ModifiedDate DATETIME,
    PRIMARY KEY (EmployeeID, AddressID)
);

INSERT INTO
    dbo.EmployeeAddress (
        EmployeeID,
        AddressID,
        AddressTypeID,
        AddressLine1,
        AddressLine2,
        City,
        StateProvinceID,
        PostalCode,
        SpatialLocation,
        ModifiedDate
    )
SELECT
    CTE.EmployeeID,
    T.AddressID,
    T.AddressTypeID,
    S.AddressLine1,
    S.AddressLine2,
    S.City,
    S.StateProvinceID,
    S.PostalCode,
    S.SpatialLocation,
    CASE
        WHEN T.ModifiedDate > S.ModifiedDate THEN T.ModifiedDate
        ELSE S.ModifiedDate
    END AS ModifiedDate
FROM
    [AdventureWorks2014].[Person].[BusinessEntityAddress] T
    INNER JOIN [AdventureWorks2014].[Person].[Address] S ON T.AddressID = S.AddressID
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
    ) AS CTE ON CTE.BusinessEntityID = T.BusinessEntityID
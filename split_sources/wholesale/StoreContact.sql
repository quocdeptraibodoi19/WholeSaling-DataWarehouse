USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.StoreContact;

CREATE TABLE dbo.StoreContact (
    StoreID INT,
    StackHolderID INT,
    ContactTypeID INT,
    ModifiedDate DATETIME,
    PRIMARY KEY(StoreID, StackHolderID)
);

INSERT INTO
    dbo.StoreContact(
        StoreID,
        StackHolderID,
        ContactTypeID,
        ModifiedDate
    )
SELECT
    CTE.StoreID,
    StakeHolder.StackHolderID,
    s.ContactTypeID,
    s.ModifiedDate
FROM
    [AdventureWorks2014].[Person].[BusinessEntityContact] s
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS StackHolderID,
            T.BusinessEntityID,
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
            ModifiedDate
        FROM
            [AdventureWorks2014].[Person].[Person] T
        WHERE
            PersonType IN ('VC', 'GC', 'SC')
    ) StakeHolder ON StakeHolder.BusinessEntityID = s.BusinessEntityID
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS StoreID,
            S.BusinessEntityID,
            [Name],
            T.NationalIDNumber AS EmployeeNationalIDNumber,
            CONVERT(NVARCHAR(MAX), [Demographics]) AS Demographics,
            K.[AddressLine1],
            K.[AddressLine2],
            K.[City],
            K.[PostalCode],
            CONVERT(NVARCHAR(MAX), K.[SpatialLocation]) AS SpatialLocation,
            K.[StateProvinceID],
            S.[ModifiedDate]
        FROM
            [AdventureWorks2014].[Sales].[Store] S
            INNER JOIN [AdventureWorks2014].[HumanResources].[Employee] T ON S.SalesPersonID = T.BusinessEntityID
            INNER JOIN (
                SELECT
                    S.AddressID,
                    S.BusinessEntityID,
                    S.AddressTypeID,
                    K.AddressLine1,
                    K.AddressLine2,
                    K.City,
                    K.PostalCode,
                    K.SpatialLocation,
                    K.StateProvinceID
                FROM
                    [AdventureWorks2014].[Person].[BusinessEntityAddress] S
                    INNER JOIN [AdventureWorks2014].[Person].[Address] K ON K.AddressID = S.AddressID
            ) K ON S.BusinessEntityID = K.BusinessEntityID
    ) CTE ON CTE.BusinessEntityID = s.BusinessEntityID
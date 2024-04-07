USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.UserAddress;

CREATE TABLE dbo.UserAddress (
    AddressID INT,
    UserID INT,
    AddressTypeID INT,
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(255),
    StateProvinceID INT,
    PostalCode NVARCHAR(15),
    SpatialLocation GEOGRAPHY,
    ModifiedDate DATETIME,
    PRIMARY KEY(AddressID, UserID)
);

INSERT INTO
    dbo.UserAddress (
        AddressID,
        UserID,
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
    S.AddressID,
    CTE.UserID,
    T.AddressTypeID,
    S.AddressLine1,
    S.AddressLine2,
    S.City,
    S.StateProvinceID,
    S.PostalCode,
    S.SpatialLocation,
    CASE
        WHEN S.ModifiedDate > T.ModifiedDate THEN S.ModifiedDate
        ELSE T.ModifiedDate
    END AS ModifiedDate
FROM
    [AdventureWorks2014].[Person].[Address] S
    INNER JOIN [AdventureWorks2014].[Person].[BusinessEntityAddress] T ON S.AddressID = T.AddressID
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS UserID,
            [PersonID],
            [AccountNumber],
            [TerritoryID],
            [NameStyle],
            [Title],
            [FirstName],
            [MiddleName],
            [LastName],
            [Suffix],
            [EmailPromotion],
            CONVERT(NVARCHAR(MAX), [AdditionalContactInfo]) AS AdditionalContactInfo,
            CONVERT(NVARCHAR(MAX), [Demographics]) AS Demographics,
            CASE
                WHEN S.[ModifiedDate] > T.[ModifiedDate] THEN S.[ModifiedDate]
                ELSE T.[ModifiedDate]
            END AS ModifiedDate
        FROM
            [AdventureWorks2014].[Sales].[Customer] S
            INNER JOIN [AdventureWorks2014].[Person].[Person] T ON S.PersonID = T.BusinessEntityID
        WHERE
            S.StoreID IS NULL
    ) AS CTE ON CTE.PersonID = T.BusinessEntityID;
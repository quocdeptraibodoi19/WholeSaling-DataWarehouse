USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.StackHolderAddress;

CREATE TABLE dbo.StackHolderAddress (
    StackHolderID INT,
    AddressID INT,
    AddressTypeID INT,
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(50),
    StateProvinceID INT,
    PostalCode NVARCHAR(15),
    SpatialLocation GEOGRAPHY,
    -- Adjust the data type based on your needs
    ModifiedDate DATETIME,
    PRIMARY KEY (StackHolderID, AddressID) -- Adjust the primary key based on your needs
);

INSERT INTO
    dbo.StackHolderAddress (
        StackHolderID,
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
    CTE.StackHolderID,
    T.AddressID,
    T.AddressTypeID,
    S.AddressLine1,
    S.AddressLine2,
    S.City,
    S.StateProvinceID,
    S.PostalCode,
    S.SpatialLocation,
    CASE
        WHEN T.ModifiedDate > S.ModifiedDate
        AND T.ModifiedDate > CTE.ModifiedDate THEN T.ModifiedDate
        WHEN S.ModifiedDate > T.ModifiedDate
        AND S.ModifiedDate > CTE.ModifiedDate THEN S.ModifiedDate
        ELSE CTE.ModifiedDate
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
            ) AS StackHolderID,
            T.BusinessEntityID,
            PersonType,
            NameStyle,
            Title,
            FirstName,
            MiddleName,
            LastName,
            Suffix,
            S.ContactTypeID as PositionTypeID,
            EmailPromotion,
            AdditionalContactInfo,
            Demographics,
            CASE
                WHEN T.ModifiedDate > S.ModifiedDate THEN T.ModifiedDate
                ELSE S.ModifiedDate
            END AS ModifiedDate
        FROM
            [AdventureWorks2014].[Person].[Person] T
            INNER JOIN [AdventureWorks2014].[Person].[BusinessEntityContact] S ON T.BusinessEntityID = S.PersonID
        WHERE
            PersonType IN ('SP', 'VC', 'GC', "SC");

) AS CTE ON CTE.BusinessEntityID = T.BusinessEntityID
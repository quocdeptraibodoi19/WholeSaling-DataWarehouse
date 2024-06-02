USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.StoreAddress;

CREATE TABLE dbo.StoreAddress (
    AddressID INT,
    StoreID INT,
    AddressTypeID INT,
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(255),
    StateProvinceID INT,
    PostalCode NVARCHAR(15),
    SpatialLocation GEOGRAPHY,
    ModifiedDate DATETIME,
    PRIMARY KEY(AddressID, StoreID)
);

INSERT INTO
    dbo.StoreAddress (
        AddressID,
        StoreID,
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
    s.AddressID,
    k.StoreID,
    t.AddressTypeID,
    s.AddressLine1,
    s.AddressLine2,
    s.City,
    s.StateProvinceID,
    s.PostalCode,
    s.SpatialLocation,
    t.ModifiedDate
FROM
    [AdventureWorks2014].[Person].[BusinessEntityAddress] t
    INNER JOIN [AdventureWorks2014].[Person].[Address] s ON t.AddressID = s.AddressID
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
            S.[ModifiedDate]
        FROM
            [AdventureWorks2014].[Sales].[Store] S
            INNER JOIN [AdventureWorks2014].[HumanResources].[Employee] T ON S.SalesPersonID = T.BusinessEntityID
    ) k on k.BusinessEntityID = t.BusinessEntityID
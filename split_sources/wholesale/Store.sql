/*
    User for constructing the BussinessEntityAddress
    Making assumption that each EmployeNationalIdNumber is unique
*/
USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.Store;

CREATE TABLE dbo.Store (
    StoreID INT PRIMARY KEY,
    Name NVARCHAR(255),
    EmployeeNationalIDNumber NVARCHAR(15),
    Demographics NVARCHAR(MAX),
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(255),
    PostalCode NVARCHAR(15),
    SpatialLocation NVARCHAR(MAX),
    StateProvinceID INT,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Store (
        StoreID,
        Name,
        EmployeeNationalIDNumber,
        Demographics,
        AddressLine1,
        AddressLine2,
        City,
        PostalCode,
        SpatialLocation,
        StateProvinceID,
        ModifiedDate
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS StoreID,
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
    ) K ON S.BusinessEntityID = K.BusinessEntityID;
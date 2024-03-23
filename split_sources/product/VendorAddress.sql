USE Product;

GO
    DROP TABLE IF EXISTS dbo.VendorAddress;

CREATE TABLE dbo.VendorAddress (
    VendorID INT,
    AddressID INT,
    AddressTypeID INT,
    AddressLine1 NVARCHAR(100),
    AddressLine2 NVARCHAR(100),
    City NVARCHAR(50),
    StateProvinceID INT,
    PostalCode NVARCHAR(15),
    SpatialLocation GEOGRAPHY,
    ModifiedDate DATETIME,
    PRIMARY KEY (VendorID, AddressID, AddressTypeID)
);

INSERT INTO
    dbo.VendorAddress (
        VendorID,
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
    CTE.VendorID,
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
            ) AS VendorID,
            BusinessEntityID,
            [AccountNumber],
            [Name],
            [CreditRating],
            [PreferredVendorStatus],
            [ActiveFlag],
            [PurchasingWebServiceURL],
            [ModifiedDate]
        FROM
            [AdventureWorks2014].[Purchasing].[Vendor]
    ) AS CTE ON CTE.BusinessEntityID = T.BusinessEntityID
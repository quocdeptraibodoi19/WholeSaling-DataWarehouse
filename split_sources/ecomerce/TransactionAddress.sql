USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.TransactionAddress;

CREATE TABLE dbo.TransactionAddress (
    AddressID INT PRIMARY KEY,
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(50),
    StateProvinceID INT,
    PostalCode NVARCHAR(15),
    SpatialLocation GEOGRAPHY,
    ModifiedDate DATETIME,
);

WITH CTE AS (
    SELECT
        [BillToAddressID],
        [ShipToAddressID]
    FROM
        [AdventureWorks2014].[Sales].[SalesOrderHeader]
    WHERE
        [OnlineOrderFlag] = 1
)
INSERT INTO
    dbo.TransactionAddress (
        AddressID,
        AddressLine1,
        AddressLine2,
        City,
        StateProvinceID,
        PostalCode,
        SpatialLocation,
        ModifiedDate
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS AddressID,
    [AddressLine1],
    [AddressLine2],
    [City],
    [StateProvinceID],
    [PostalCode],
    [SpatialLocation],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Person].[Address]
WHERE
    [AddressID] IN (
        SELECT
            [BillToAddressID]
        FROM
            CTE
    )
    OR [AddressID] IN (
        SELECT
            [ShipToAddressID]
        FROM
            CTE
    );
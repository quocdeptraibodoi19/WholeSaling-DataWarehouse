USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.SalesOrderHeader;

CREATE TABLE dbo.SalesOrderHeader (
    SalesOrderID INT PRIMARY KEY,
    RevisionNumber INT,
    OrderDate DATETIME,
    DueDate DATETIME,
    ShipDate DATETIME,
    Status INT,
    SalesOrderNumber NVARCHAR(50),
    PurchaseOrderNumber NVARCHAR(50),
    AccountNumber NVARCHAR(15),
    CustomerID INT,
    SaleEmployeeNationalNumberID NVARCHAR(15),
    TerritoryID INT,
    BillToAddressID INT,
    ShipToAddressID INT,
    ShipMethodID INT,
    CreditCardID INT,
    CreditCardApprovalCode NVARCHAR(50),
    CurrencyRateID INT,
    SubTotal MONEY,
    TaxAmt MONEY,
    Freight MONEY,
    TotalDue MONEY,
    ModifiedDate DATETIME
);

WITH CTE AS (
    SELECT
        [BillToAddressID],
        [ShipToAddressID]
    FROM
        [AdventureWorks2014].[Sales].[SalesOrderHeader]
    WHERE
        [OnlineOrderFlag] = 0
),
TransactionAddress AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                (
                    SELECT
                        NULL
                )
        ) AS AddressID,
        [AddressID] AS OldAddressID,
        [AddressLine1],
        [AddressLine2],
        [City],
        [StateProvinceID],
        [PostalCode],
        [SpatialLocation],
        [rowguid],
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
        )
)
INSERT INTO
    dbo.SalesOrderHeader (
        [SalesOrderID],
        [RevisionNumber],
        [OrderDate],
        [DueDate],
        [ShipDate],
        [Status],
        [SalesOrderNumber],
        [PurchaseOrderNumber],
        [AccountNumber],
        [CustomerID],
        [SaleEmployeeNationalNumberID],
        [TerritoryID],
        [BillToAddressID],
        [ShipToAddressID],
        [ShipMethodID],
        [CreditCardID],
        [CreditCardApprovalCode],
        [CurrencyRateID],
        [SubTotal],
        [TaxAmt],
        [Freight],
        [TotalDue],
        [ModifiedDate]
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS SalesOrderID,
    S.[RevisionNumber],
    S.[OrderDate],
    S.[DueDate],
    S.[ShipDate],
    S.[Status],
    S.[SalesOrderNumber],
    S.[PurchaseOrderNumber],
    S.[AccountNumber],
    CTE.[CustomerID],
    K.NationalIDNumber as SaleEmployeeNationalNumberID,
    S.[TerritoryID],
    T1.AddressID AS BillToAddressID,
    T2.AddressID AS ShipToAddressID,
    S.[ShipMethodID],
    S.[CreditCardID],
    S.[CreditCardApprovalCode],
    S.[CurrencyRateID],
    S.[SubTotal],
    S.[TaxAmt],
    S.[Freight],
    S.[TotalDue],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesOrderHeader] S
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS CustomerID,
            S.CustomerID AS oldCustomerID,
            CTE2.StackHolderID AS StoreRepID,
            CTE.[StoreID],
            [TerritoryID],
            [AccountNumber],
            S.[ModifiedDate]
        FROM
            [AdventureWorks2014].[Sales].[Customer] S
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
            ) AS CTE ON CTE.BusinessEntityID = S.StoreID
            LEFT JOIN (
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
                    LEFT JOIN [AdventureWorks2014].[Person].[BusinessEntityContact] S ON T.BusinessEntityID = S.PersonID
                WHERE
                    PersonType IN ('VC', 'GC', 'SC')
            ) AS CTE2 ON S.PersonID = CTE2.BusinessEntityID
        WHERE
            S.StoreID is Not NULL
    ) AS CTE ON S.CustomerID = CTE.oldCustomerID
    INNER JOIN [AdventureWorks2014].[HumanResources].[Employee] K ON K.BusinessEntityID = S.SalesPersonID
    INNER JOIN TransactionAddress T1 ON T1.OldAddressID = S.BillToAddressID
    INNER JOIN TransactionAddress T2 ON T2.OldAddressID = S.ShipToAddressID
WHERE
    S.[OnlineOrderFlag] = 0
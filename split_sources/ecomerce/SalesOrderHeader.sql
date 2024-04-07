USE Ecomerce;

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
    AccountNumber NVARCHAR(50),
    UserID INT,
    TerritoryID INT,
    BillToAddressID INT,
    ShipToAddressID INT,
    ShipMethodID INT,
    CardNumber NVARCHAR(255),
    CardType NVARCHAR(50),
    ExpMonth INT,
    ExpYear INT,
    CreditCardApprovalCode NVARCHAR(50),
    CurrencyRateID INT,
    SubTotal MONEY,
    TaxAmt MONEY,
    Freight MONEY,
    TotalDue MONEY,
    Comment NVARCHAR(MAX),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.SalesOrderHeader (
        [SalesOrderID],
        [RevisionNumber],
        [OrderDate],
        [DueDate],
        [ShipDate],
        [Status],
        [SalesOrderNumber],
        [AccountNumber],
        UserID,
        [TerritoryID],
        [BillToAddressID],
        [ShipToAddressID],
        [ShipMethodID],
        [CardNumber],
        [CardType],
        [ExpMonth],
        [ExpYear],
        [CreditCardApprovalCode],
        [CurrencyRateID],
        [SubTotal],
        [TaxAmt],
        [Freight],
        [TotalDue],
        [Comment],
        [ModifiedDate]
    )
SELECT
    [SalesOrderID],
    [RevisionNumber],
    [OrderDate],
    [DueDate],
    [ShipDate],
    [Status],
    [SalesOrderNumber],
    S.[AccountNumber],
    CTE.UserID,
    S.[TerritoryID],
    [BillToAddressID],
    [ShipToAddressID],
    [ShipMethodID],
    Q.CardNumber,
    Q.CardType,
    Q.ExpMonth,
    Q.ExpYear,
    [CreditCardApprovalCode],
    [CurrencyRateID],
    [SubTotal],
    [TaxAmt],
    [Freight],
    [TotalDue],
    [Comment],
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
            ) AS UserID,
            CustomerID AS OldCustomerID,
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
    ) AS CTE ON CTE.OldCustomerID = S.CustomerID
    LEFT JOIN [AdventureWorks2014].[Sales].[CreditCard] Q ON Q.CreditCardID = S.CreditCardID
WHERE
    OnlineOrderFlag = 1;
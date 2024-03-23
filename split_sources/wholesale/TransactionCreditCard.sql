USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.TransactionCreditCard;

CREATE TABLE dbo.TransactionCreditCard (
    CreditCardID INT,
    CardNumber NVARCHAR(255),
    CardType NVARCHAR(50),
    ExpMonth INT,
    ExpYear INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (StoreRepID, CardNumber)
);

INSERT INTO
    dbo.TransactionCreditCard (
        CreditCardID,
        CardNumber,
        CardType,
        ExpMonth,
        ExpYear,
        ModifiedDate
    )
SELECT
    [CreditCardID],
    [CardType],
    [CardNumber],
    [ExpMonth],
    [ExpYear],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[CreditCard]
WHERE
    S.CreditCardID IN (
        SELECT
            CreditCardID
        FROM
            [AdventureWorks2014].[Sales].[SalesOrderHeader]
        WHERE
            OnlineOrderFlag = 0
    )
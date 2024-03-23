USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.UserCreditCard;

CREATE TABLE dbo.UserCreditCard (
    UserID INT,
    CreditCardID INT,
    CardNumber NVARCHAR(255),
    CardType NVARCHAR(50),
    ExpMonth INT,
    ExpYear INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (UserID, CreditCardID)
);

INSERT INTO
    dbo.UserCreditCard (
        UserID,
        [CreditCardID],
        CardNumber,
        CardType,
        ExpMonth,
        ExpYear,
        ModifiedDate
    )
SELECT
    CTE.UserID,
    S.[CreditCardID],
    T.CardNumber,
    T.CardType,
    T.ExpMonth,
    T.ExpYear,
CASE
        WHEN S.[ModifiedDate] > T.[ModifiedDate] THEN S.[ModifiedDate]
        ELSE T.[ModifiedDate]
    END
FROM
    [AdventureWorks2014].[Sales].[PersonCreditCard] S
    INNER JOIN [AdventureWorks2014].[Sales].[CreditCard] T ON S.CreditCardID = T.CreditCardID
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
    ) AS CTE ON CTE.PersonID = S.BusinessEntityID
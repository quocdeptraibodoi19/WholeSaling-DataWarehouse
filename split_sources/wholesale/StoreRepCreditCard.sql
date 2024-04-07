USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.StoreRepCreditCard;

CREATE TABLE dbo.StoreRepCreditCard (
    StoreRepID INT,
    CreditCardID INT,
    CardNumber NVARCHAR(255),
    CardType NVARCHAR(50),
    ExpMonth INT,
    ExpYear INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (StoreRepID, CreditCardID)
);

INSERT INTO
    dbo.StoreRepCreditCard (
        StoreRepID,
        CreditCardID,
        CardNumber,
        CardType,
        ExpMonth,
        ExpYear,
        ModifiedDate
    )
SELECT
    CTE.StackHolderID AS StoreRepID,
    S.CreditCardID,
    T.CardNumber,
    T.CardType,
    T.ExpMonth,
    T.ExpYear,
CASE
        WHEN S.[ModifiedDate] > T.[ModifiedDate] THEN S.[ModifiedDate]
        ELSE T.[ModifiedDate]
    END AS ModifiedDate
FROM
    [AdventureWorks2014].[Sales].[PersonCreditCard] S
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
            EmailPromotion,
            AdditionalContactInfo,
            Demographics,
            ModifiedDate
        FROM
            [AdventureWorks2014].[Person].[Person] T
        WHERE
            PersonType IN ('VC', 'GC', 'SC')
    ) AS CTE ON S.BusinessEntityID = CTE.BusinessEntityID
    AND CTE.PersonType = 'SC'
    INNER JOIN [AdventureWorks2014].[Sales].[CreditCard] T ON S.CreditCardID = T.CreditCardID
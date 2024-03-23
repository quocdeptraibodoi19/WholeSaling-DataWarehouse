USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.StoreRepCreditCard;

CREATE TABLE dbo.StoreRepCreditCard (
    StoreRepID INT,
    CardNumber NVARCHAR(255),
    CardType NVARCHAR(50),
    ExpMonth INT,
    ExpYear INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (StoreRepID, CardNumber)
);

INSERT INTO
    dbo.StoreRepCreditCard (
        StoreRepID,
        CardNumber,
        CardType,
        ExpMonth,
        ExpYear,
        ModifiedDate
    )
SELECT
    CTE.StackHolderID AS StoreRepID,
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
            PersonType IN ('SP', 'VC', 'GC', "SC")
    ) AS CTE ON S.BusinessEntityID = CTE.BusinessEntityID
    AND CTE.PersonType = 'SC'
    INNER JOIN [AdventureWorks2014].[Sales].[CreditCard] T ON S.CreditCardID = T.CreditCardID
USE HumanResourceSystem;

GO

DROP TABLE IF EXISTS dbo.StackholderPhoneContact;

    CREATE TABLE dbo.StackholderPhoneContact (
        StackHolderID INT PRIMARY KEY,
        PhoneNumber NVARCHAR(20),
        PhoneNumberTypeID INT,
        ModifiedDate DATETIME
    );

INSERT INTO
    dbo.StackholderPhoneContact (
        StackHolderID,
        PhoneNumber,
        PhoneNumberTypeID,
        ModifiedDate
    )
SELECT
    CTE.StackHolderID,
    T.PhoneNumber,
    T.PhoneNumberTypeID,
    T.ModifiedDate
FROM
    [AdventureWorks2014].[Person].[PersonPhone] AS T
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS StackHolderID,
            PersonType,
            T.BusinessEntityID,
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
    ) AS CTE ON CTE.BusinessEntityID = T.BusinessEntityID;
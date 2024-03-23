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
            INNER JOIN [AdventureWorks2014].[Person].[BusinessEntityContact] S ON T.BusinessEntityID = S.PersonID
        WHERE
            PersonType IN ('SP', 'VC', 'GC', "SC");
    ) AS CTE ON CTE.BusinessEntityID = T.BusinessEntityID;
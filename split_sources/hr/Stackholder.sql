/*
 For constructing Person,
 Just this table is used to construct
 */
Use HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.StakeHolder;

CREATE TABLE [dbo].[StakeHolder] (
    StackHolderID INT PRIMARY KEY,
    PersonType NVARCHAR(2),
    NameStyle BIT,
    Title NVARCHAR(8),
    FirstName NVARCHAR(50),
    MiddleName NVARCHAR(50),
    LastName NVARCHAR(50),
    Suffix NVARCHAR(10),
    PositionTypeID INT,
    EmailPromotion INT,
    AdditionalContactInfo XML,
    Demographics XML,
    ModifiedDate DATETIME
)
INSERT INTO
    [dbo].[StakeHolder] (
        StackHolderID,
        PersonType,
        NameStyle,
        Title,
        FirstName,
        MiddleName,
        LastName,
        Suffix,
        PositionTypeID,
        EmailPromotion,
        AdditionalContactInfo,
        Demographics,
        ModifiedDate
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS StackHolderID,
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
    PersonType IN ('VC', 'GC', 'SC');
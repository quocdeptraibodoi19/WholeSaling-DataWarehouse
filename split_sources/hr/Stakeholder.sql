/*
 For constructing Person,
 Just this table is used to construct
 */
Use HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.Stakeholder;

CREATE TABLE [dbo].[Stakeholder] (
    StackHolderID INT PRIMARY KEY,
    PersonType NVARCHAR(2),
    NameStyle BIT,
    Title NVARCHAR(8),
    FirstName NVARCHAR(50),
    MiddleName NVARCHAR(50),
    LastName NVARCHAR(50),
    Suffix NVARCHAR(10),
    EmailPromotion INT,
    AdditionalContactInfo XML,
    Demographics XML,
    ModifiedDate DATETIME
)
INSERT INTO
    [dbo].[Stakeholder] (
        StackHolderID,
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
    EmailPromotion,
    AdditionalContactInfo,
    Demographics,
    ModifiedDate
FROM
    [AdventureWorks2014].[Person].[Person] T
WHERE
    PersonType IN ('VC', 'GC', 'SC');
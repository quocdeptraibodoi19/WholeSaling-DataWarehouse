/*
 For constructing Person
 */
USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.[User];

CREATE TABLE dbo.[User] (
    UserID INT PRIMARY KEY,
    AccountNumber NVARCHAR(50),
    TerritoryID INT,
    NameStyle BIT,
    Title NVARCHAR(50),
    FirstName NVARCHAR(50),
    MiddleName NVARCHAR(50),
    LastName NVARCHAR(50),
    Suffix NVARCHAR(10),
    EmailPromotion INT,
    AdditionalContactInfo NVARCHAR(MAX),
    Demographics NVARCHAR(MAX),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.[User] (
        [UserID],
        [AccountNumber],
        [TerritoryID],
        [NameStyle],
        [Title],
        [FirstName],
        [MiddleName],
        [LastName],
        [Suffix],
        [EmailPromotion],
        [AdditionalContactInfo],
        [Demographics],
        [ModifiedDate]
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS UserID,
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
    S.StoreID IS NULL;
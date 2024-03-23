USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.UserPassword;

CREATE TABLE dbo.[UserPassword](
    UserID INT PRIMARY KEY,
    PasswordHash NVARCHAR(100),
    PasswordSalt NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.[UserPassword] (
        [UserID],
        [PasswordHash],
        [PasswordSalt],
        [ModifiedDate]
    )
SELECT
    t.UserID,
    s.PasswordHash,
    s.PasswordSalt,
    s.ModifiedDate
FROM
    [AdventureWorks2014].[Person].[Password] s
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS UserID,
            T.BusinessEntityID,
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
    ) t ON s.BusinessEntityID = t.BusinessEntityID;
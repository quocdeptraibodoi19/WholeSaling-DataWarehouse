Use HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.StackholderPassword;

CREATE TABLE dbo.[StackholderPassword](
    StackHolderID INT PRIMARY KEY,
    PasswordHash NVARCHAR(100),
    PasswordSalt NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.[StackholderPassword] (
        [StackHolderID],
        [PasswordHash],
        [PasswordSalt],
        [ModifiedDate]
    )
SELECT
    t.StackHolderID,
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
    ) t ON t.BusinessEntityID = s.BusinessEntityID
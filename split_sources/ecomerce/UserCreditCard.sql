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
            cast(CONVERT(datetime, LEFT(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";BirthDate','varchar(20)'), 10)) as varchar(20)) AS [BirthDate],
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";MaritalStatus','varchar(1)') as varchar(1)) AS [MaritalStatus], 	
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";Gender','varchar(1)') AS varchar(1)) AS [Gender], 
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";TotalChildren','int') AS int) AS [TotalChildren], 
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";NumberChildrenAtHome','int') AS int)  AS [NumberChildrenAtHome], 
            CAST(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";HomeOwnerFlag','int') AS varchar(1)) AS [HouseOwnerFlag], 
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";NumberCarsOwned','int') AS INT) AS [NumberCarsOwned], 
            CONVERT(datetime, LEFT(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";DateFirstPurchase','varchar(20)'), 10)) AS [DateFirstPurchase],     
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";CommuteDistance','varchar(15)') AS varchar(15)) AS [CommuteDistance],
            cast([Demographics].value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";(IndividualSurvey/Education)[1]','varchar(40)') AS varchar(40)) as [Education],
            cast([Demographics].value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";(IndividualSurvey/Occupation)[1]','varchar(40)') AS varchar(40)) as [Occupation],
            CASE
                WHEN S.[ModifiedDate] > T.[ModifiedDate] THEN S.[ModifiedDate]
                ELSE T.[ModifiedDate]
            END AS ModifiedDate
        FROM
            [AdventureWorks2014].[Sales].[Customer] S
            INNER JOIN [AdventureWorks2014].[Person].[Person] T ON S.PersonID = T.BusinessEntityID
            cross apply [Demographics].nodes(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";IndividualSurvey') AS Survey(ref)
        WHERE
            S.StoreID IS NULL
    ) AS CTE ON CTE.PersonID = S.BusinessEntityID
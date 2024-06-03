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
    BirthDate VARCHAR(20),
    MaritalStatus VARCHAR(1),
    Gender VARCHAR(1),
    TotalChildren INT,
    NumberChildrenAtHome INT,
    HouseOwnerFlag VARCHAR(1),
    NumberCarsOwned INT,
    DateFirstPurchase VARCHAR(20),
    CommuteDistance VARCHAR(15),
    Education VARCHAR(40),
    Occupation VARCHAR(40),
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
        [BirthDate],
        [MaritalStatus],
        [Gender],
        [TotalChildren],
        [NumberChildrenAtHome],
        [HouseOwnerFlag],
        [NumberCarsOwned],
        [DateFirstPurchase],
        [CommuteDistance],
        [Education],
        [Occupation],
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
    S.StoreID IS NULL;
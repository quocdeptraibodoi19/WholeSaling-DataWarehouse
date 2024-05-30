USE Product;

GO
    DROP TABLE IF EXISTS dbo.VendorContact;

CREATE TABLE dbo.VendorContact (
    VendorID INT,
    StackHolderID INT,
    ContactTypeID INT,
    ModifiedDate DATETIME,
    PRIMARY KEY(VendorID, StackHolderID)
);

INSERT INTO
    dbo.VendorContact(
        VendorID,
        StackHolderID,
        ContactTypeID,
        ModifiedDate
    )
SELECT
    CTE.VendorID,
    StakeHolder.StackHolderID,
    s.ContactTypeID,
    s.ModifiedDate
FROM
    [AdventureWorks2014].[Person].[BusinessEntityContact] s
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
            cast(CONVERT(datetime, LEFT(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";BirthDate','varchar(20)'), 10)) as varchar(20)) AS [BirthDate],
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";MaritalStatus','varchar(1)') as varchar(1)) AS [MaritalStatus], 	
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";Gender','varchar(1)') AS varchar(1)) AS [Gender], 
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";TotalChildren','int') AS int) AS [TotalChildren], 
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";NumberChildrenAtHome','int') AS int)  AS [NumberChildrenAtHome], 
            CAST(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";HomeOwnerFlag','int') AS varchar(1)) AS [HouseOwnerFlag], 
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";NumberCarsOwned','int') AS INT) AS [NumberCarsOwned], 
            CONVERT(datetime, LEFT(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";DateFirstPurchase','varchar(20)'), 10)) AS [DateFirstPurchase],     
            cast(Survey.ref.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";CommuteDistance','varchar(15)') AS varchar(15)) AS [CommuteDistance],
            cast(Demographics.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";(IndividualSurvey/Education)[1]','varchar(40)') AS varchar(40)) as [Education],
            cast(Demographics.value(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";(IndividualSurvey/Occupation)[1]','varchar(40)') AS varchar(40)) as [Occupation],
            ModifiedDate
        FROM
            [AdventureWorks2014].[Person].[Person] T
        cross apply T.[Demographics].nodes(N'declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey";IndividualSurvey') AS Survey(ref)
        WHERE
            PersonType IN ('VC', 'GC', 'SC')
    ) AS StakeHolder ON StakeHolder.BusinessEntityID = s.BusinessEntityID
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS VendorID,
            [BusinessEntityID],
            [AccountNumber],
            [Name],
            [CreditRating],
            [PreferredVendorStatus],
            [ActiveFlag],
            [PurchasingWebServiceURL],
            [ModifiedDate]
        FROM
            [AdventureWorks2014].[Purchasing].[Vendor]
    ) AS CTE ON CTE.BusinessEntityID = s.BusinessEntityID
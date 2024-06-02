USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.SalesOrderDetail;

CREATE TABLE dbo.SalesOrderDetail (
    SalesOrderID INT,
    SalesOrderDetailID INT PRIMARY KEY,
    CarrierTrackingNumber NVARCHAR(25),
    OrderQty INT,
    ProductID INT,
    SpecialOfferID INT,
    UnitPrice MONEY,
    UnitPriceDiscount MONEY,
    LineTotal MONEY,
    ModifiedDate DATETIME,
);

INSERT INTO
    dbo.SalesOrderDetail (
        SalesOrderID,
        [SalesOrderDetailID],
        [CarrierTrackingNumber],
        [OrderQty],
        [ProductID],
        [SpecialOfferID],
        [UnitPrice],
        [UnitPriceDiscount],
        [LineTotal],
        [ModifiedDate]
    )
SELECT
    CTE1.SalesOrderID,
    [SalesOrderDetailID],
    [CarrierTrackingNumber],
    [OrderQty],
    CTE.[ProductID],
    [SpecialOfferID],
    [UnitPrice],
    [UnitPriceDiscount],
    [LineTotal],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesOrderDetail] S
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS ProductID,
            [ProductID] AS OldProductID,
            [Name],
            [ProductNumber],
            [MakeFlag],
            [FinishedGoodsFlag],
            [Color],
            [SafetyStockLevel],
            [ReorderPoint],
            [StandardCost],
            [ListPrice],
            [Size],
            [SizeUnitMeasureCode],
            [WeightUnitMeasureCode],
            [Weight],
            [DaysToManufacture],
            [ProductLine],
            [Class],
            [Style],
            [ProductSubcategoryID],
            [ProductModelID],
            [SellStartDate],
            [SellEndDate],
            [DiscontinuedDate],
            [ModifiedDate]
        FROM
            [AdventureWorks2014].[Production].[Product]
    ) AS CTE ON CTE.OldProductID = S.ProductID
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS SalesOrderID,
            S.[SalesOrderID] AS OldSalesOrderID,
            S.[RevisionNumber],
            S.[OrderDate],
            S.[DueDate],
            S.[ShipDate],
            S.[Status],
            S.[SalesOrderNumber],
            S.[PurchaseOrderNumber],
            S.[AccountNumber],
            CTE.[CustomerID],
            K.NationalIDNumber as SaleEmployeeNationalNumberID,
            S.[TerritoryID],
            BillToAddressID,
            ShipToAddressID,
            S.[ShipMethodID],
            S.[CreditCardID],
            S.[CreditCardApprovalCode],
            S.[CurrencyRateID],
            S.[SubTotal],
            S.[TaxAmt],
            S.[Freight],
            S.[TotalDue],
            S.[ModifiedDate]
        FROM
            [AdventureWorks2014].[Sales].[SalesOrderHeader] S
            INNER JOIN (
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY
                            (
                                SELECT
                                    NULL
                            )
                    ) AS CustomerID,
                    S.CustomerID AS oldCustomerID,
                    CTE2.StackHolderID AS StoreRepID,
                    CTE.[StoreID],
                    [TerritoryID],
                    [AccountNumber],
                    S.[ModifiedDate]
                FROM
                    [AdventureWorks2014].[Sales].[Customer] S
                    INNER JOIN (
                        SELECT
                            ROW_NUMBER() OVER (
                                ORDER BY
                                    (
                                        SELECT
                                            NULL
                                    )
                            ) AS StoreID,
                            S.BusinessEntityID,
                            [Name],
                            T.NationalIDNumber AS EmployeeNationalIDNumber,
                            CONVERT(NVARCHAR(MAX), [Demographics]) AS Demographics,
                            S.[ModifiedDate]
                        FROM
                            [AdventureWorks2014].[Sales].[Store] S
                            INNER JOIN [AdventureWorks2014].[HumanResources].[Employee] T ON S.SalesPersonID = T.BusinessEntityID
                    ) AS CTE ON CTE.BusinessEntityID = S.StoreID
                    LEFT JOIN (
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
                    ) AS CTE2 ON S.PersonID = CTE2.BusinessEntityID
                WHERE
                    S.StoreID is Not NULL
            ) AS CTE ON S.CustomerID = CTE.oldCustomerID
            INNER JOIN [AdventureWorks2014].[HumanResources].[Employee] K ON K.BusinessEntityID = S.SalesPersonID
        WHERE
            S.[OnlineOrderFlag] = 0
    ) AS CTE1 ON CTE1.OldSalesOrderID = S.SalesOrderID
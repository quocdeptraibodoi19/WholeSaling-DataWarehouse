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
        WITH TransactionAddress AS (
            WITH CTE AS (
                SELECT
                    [BillToAddressID],
                    [ShipToAddressID]
                FROM
                    [AdventureWorks2014].[Sales].[SalesOrderHeader]
                WHERE
                    [OnlineOrderFlag] = 0
            )
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY
                        (
                            SELECT
                                NULL
                        )
                ) AS AddressID,
                [AddressID] AS OldAddressID,
                [AddressLine1],
                [AddressLine2],
                [City],
                [StateProvinceID],
                [PostalCode],
                [SpatialLocation],
                [rowguid],
                [ModifiedDate]
            FROM
                [AdventureWorks2014].[Person].[Address]
            WHERE
                [AddressID] IN (
                    SELECT
                        [BillToAddressID]
                    FROM
                        CTE
                )
                OR [AddressID] IN (
                    SELECT
                        [ShipToAddressID]
                    FROM
                        CTE
                )
        )
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
            T1.AddressID AS BillToAddressID,
            T2.AddressID AS ShipToAddressID,
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

) AS CTE2 ON S.PersonID = CTE2.BusinessEntityID
WHERE
    S.StoreID is Not NULL
) AS CTE ON S.CustomerID = CTE.oldCustomerID
INNER JOIN [AdventureWorks2014].[HumanResources].[Employee] K ON K.BusinessEntityID = S.SalesPersonID
INNER JOIN CTE T1 ON T1.OldAddressID = S.BillToAddressID
INNER JOIN CTE T2 ON T2.OldAddressID = S.ShipToAddressID
LEFT JOIN ()
WHERE
    S.[OnlineOrderFlag] = 0
) AS CTE1 ON CTE1.OldSalesOrderID = S.SalesOrderID
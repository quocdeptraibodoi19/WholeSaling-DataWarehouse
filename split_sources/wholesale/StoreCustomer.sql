USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.StoreCustomer;

CREATE TABLE dbo.StoreCustomer (
    CustomerID INT PRIMARY KEY,
    StoreRepID INT,
    StoreID INT,
    TerritoryID INT,
    AccountNumber NVARCHAR(255),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.StoreCustomer(
        CustomerID,
        StoreRepID,
        StoreID,
        TerritoryID,
        AccountNumber,
        ModifiedDate
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS CustomerID,
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
            LEFT JOIN [AdventureWorks2014].[Person].[BusinessEntityContact] S ON T.BusinessEntityID = S.PersonID
        WHERE
            PersonType IN  ('SP', 'VC', 'GC', "SC")
    ) AS CTE2 ON S.PersonID = CTE2.BusinessEntityID
WHERE
    S.StoreID is Not NULL
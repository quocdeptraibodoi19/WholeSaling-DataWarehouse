USE Product;

GO
    DROP TABLE IF EXISTS dbo.PurchaseOrderHeader;

CREATE TABLE dbo.PurchaseOrderHeader (
    PurchaseOrderID INT PRIMARY KEY,
    RevisionNumber INT,
    Status INT,
    EmployeeNationalIDNumber NVARCHAR(15),
    VendorID INT,
    ShipMethodID INT,
    OrderDate DATETIME,
    ShipDate DATETIME,
    SubTotal MONEY,
    TaxAmt MONEY,
    Freight MONEY,
    TotalDue MONEY,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.PurchaseOrderHeader (
        [PurchaseOrderID],
        [RevisionNumber],
        [Status],
        [EmployeeNationalIDNumber],
        [VendorID],
        [ShipMethodID],
        [OrderDate],
        [ShipDate],
        [SubTotal],
        [TaxAmt],
        [Freight],
        [TotalDue],
        [ModifiedDate]
    )
SELECT
    [PurchaseOrderID],
    [RevisionNumber],
    [Status],
    CTE.[NationalIDNumber] AS EmployeeNationalIDNumber,
    CTE1.[VendorID],
    [ShipMethodID],
    [OrderDate],
    [ShipDate],
    [SubTotal],
    [TaxAmt],
    [Freight],
    [TotalDue],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Purchasing].[PurchaseOrderHeader] S
    INNER JOIN (
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    (
                        SELECT
                            NULL
                    )
            ) AS EmployeeID,
            S.EmployeeID AS OldEmployeeID,
            S.NationalIDNumber,
            S.LoginID,
            S.OrganizationNode,
            S.OrganizationLevel,
            S.Jobtitle,
            S.BirthDate,
            S.MaritalStatus,
            S.Gender,
            S.HireDate,
            S.SalariedFlag,
            S.VacationHours,
            S.SickLeaveHours,
            S.CurrentFlag,
            T.NameStyle,
            T.Title,
            T.FirstName,
            T.MiddleName,
            T.LastName,
            T.Suffix,
            T.EmailPromotion,
            T.AdditionalContactInfo,
            T.Demographics,
            K.PasswordHash,
            K.PasswordSalt,
            S.ModifiedDate
        FROM
            [AdventureWorks2014].[HumanResources].[Employee] S
            INNER JOIN [AdventureWorks2014].[Person].[Person] T ON S.BusinessEntityID = T.BusinessEntityID
            INNER JOIN [AdventureWorks2014].[Person].[Password] K ON S.BusinessEntityID = K.BusinessEntityID
    ) AS CTE ON CTE.OldEmployeeID = S.EmployeeID
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
    ) AS CTE1 ON S.VendorID = CTE1.BusinessEntityID
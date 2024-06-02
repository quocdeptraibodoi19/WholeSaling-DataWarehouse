USE Product;

GO
    DROP TABLE IF EXISTS dbo.Vendor;

CREATE TABLE dbo.Vendor (
    VendorID INT PRIMARY KEY,
    AccountNumber NVARCHAR(15),
    Name NVARCHAR(50),
    CreditRating TINYINT,
    PreferredVendorStatus BIT,
    ActiveFlag BIT,
    PurchasingWebServiceURL NVARCHAR(MAX),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Vendor (
        VendorID,
        AccountNumber,
        Name,
        CreditRating,
        PreferredVendorStatus,
        ActiveFlag,
        PurchasingWebServiceURL,
        ModifiedDate
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS VendorID,
    [AccountNumber],
    [Name],
    [CreditRating],
    [PreferredVendorStatus],
    [ActiveFlag],
    [PurchasingWebServiceURL],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Purchasing].[Vendor]
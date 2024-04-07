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
            ModifiedDate
        FROM
            [AdventureWorks2014].[Person].[Person] T
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
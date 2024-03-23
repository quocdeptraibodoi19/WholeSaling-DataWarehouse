USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.SpecialOffer;

CREATE TABLE dbo.SpecialOffer (
    SpecialOfferID INT PRIMARY KEY,
    Description NVARCHAR(255),
    DiscountPct DECIMAL(5, 2),
    Type NVARCHAR(50),
    Category NVARCHAR(50),
    StartDate DATE,
    EndDate DATE,
    MinQty INT,
    MaxQty INT,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.SpecialOffer (
        [SpecialOfferID],
        [Description],
        [DiscountPct],
        [Type],
        [Category],
        [StartDate],
        [EndDate],
        [MinQty],
        [MaxQty],
        [ModifiedDate]
    )
SELECT
    [SpecialOfferID],
    [Description],
    [DiscountPct],
    [Type],
    [Category],
    [StartDate],
    [EndDate],
    [MinQty],
    [MaxQty],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SpecialOffer]
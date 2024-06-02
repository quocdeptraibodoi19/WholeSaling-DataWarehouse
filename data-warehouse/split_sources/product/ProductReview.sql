USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductReview;

CREATE TABLE ProductReview(
    ProductReviewID INT PRIMARY KEY,
    ProductID INT,
    ReviewerName NVARCHAR(50),
    ReviewDate DATETIME,
    EmailAddress NVARCHAR(100),
    Rating INT,
    Comments NVARCHAR(MAX),
    ModifiedDate DATETIME
);

INSERT INTO
    ProductReview (
        ProductReviewID,
        ProductID,
        ReviewerName,
        ReviewDate,
        EmailAddress,
        Rating,
        Comments,
        ModifiedDate
    )
SELECT
    S.[ProductReviewID],
    CTE.[ProductID],
    S.[ReviewerName],
    S.[ReviewDate],
    S.[EmailAddress],
    S.[Rating],
    S.[Comments],
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductReview] S
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
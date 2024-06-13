USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.SpecialOfferProduct;

CREATE TABLE dbo.SpecialOfferProduct (
    SpecialOfferID INT,
    ProductID INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (SpecialOfferID, ProductID)
);

INSERT INTO
    dbo.SpecialOfferProduct (SpecialOfferID, ProductID, ModifiedDate)
SELECT
    S.SpecialOfferID,
    CTE.ProductID,
    S.ModifiedDate
FROM
    [AdventureWorks2014].[Sales].[SpecialOfferProduct] S
    INNER JOIN (
        SELECT
            [ProductID]
        FROM
            [AdventureWorks2014].[Production].[Product] s
        WHERE
            [ProductID] IN (
                SELECT
                    [ProductID]
                FROM
                    [AdventureWorks2014].[Sales].[SalesOrderDetail] Q
                    INNER JOIN [AdventureWorks2014].[Sales].[SalesOrderHeader] C ON C.SalesOrderID = Q.SalesOrderID
                WHERE
                    C.OnlineOrderFlag = 1
            )
    ) AS CTE ON CTE.ProductID = S.ProductID
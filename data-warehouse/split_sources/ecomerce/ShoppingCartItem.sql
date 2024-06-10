USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.ShoppingCartItem;

CREATE TABLE dbo.ShoppingCartItem (
    ShoppingCartItemID INT PRIMARY KEY,
    ShoppingCartID NVARCHAR(50),
    Quantity INT,
    ProductID INT,
    DateCreated DATETIME,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ShoppingCartItem (
        [ShoppingCartItemID],
        [ShoppingCartID],
        [Quantity],
        [ProductID],
        [DateCreated],
        [ModifiedDate]
    )
SELECT
    [ShoppingCartItemID],
    [ShoppingCartID],
    [Quantity],
    [ProductID],
    [DateCreated],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[ShoppingCartItem]
WHERE
    ProductID IN (
        SELECT
            ProductID
        FROM
            [AdventureWorks2014].[Production].[Product]
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
    )
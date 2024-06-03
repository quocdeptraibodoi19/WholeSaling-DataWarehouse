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
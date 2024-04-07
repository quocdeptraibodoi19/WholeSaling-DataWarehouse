USE Ecomerce;

GO
    DROP TABLE IF EXISTS dbo.SalesOrderHeaderSalesReason;

CREATE TABLE dbo.SalesOrderHeaderSalesReason (
    SalesOrderID INT,
    SalesReasonID INT,
    ModifiedDate DATETIME,
    PRIMARY KEY (SalesOrderID, SalesReasonID)
);

INSERT INTO
    dbo.SalesOrderHeaderSalesReason (
        [SalesOrderID],
        [SalesReasonID],
        [ModifiedDate]
    )
SELECT
    [SalesOrderID],
    [SalesReasonID],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesOrderHeaderSalesReason]
WHERE
    SalesOrderID IN (
        SELECT
            SalesOrderID
        FROM
            [AdventureWorks2014].[Sales].[SalesOrderHeader]
        WHERE
            OnlineOrderFlag = 1
    )
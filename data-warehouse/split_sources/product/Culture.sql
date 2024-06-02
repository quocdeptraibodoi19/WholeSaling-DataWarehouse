USE Product;

GO
    DROP TABLE IF EXISTS dbo.Culture;

CREATE TABLE dbo.Culture (
    CultureID NVARCHAR(10) PRIMARY KEY,
    Name NVARCHAR(50),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Culture (CultureID, Name, ModifiedDate)
SELECT
    [CultureID],
    [Name],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[Culture]
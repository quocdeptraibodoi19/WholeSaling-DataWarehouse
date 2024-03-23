USE Product;

GO
    DROP TABLE IF EXISTS dbo.Illustration;

CREATE TABLE dbo.Illustration (
    IllustrationID INT PRIMARY KEY,
    Diagram XML,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Illustration (IllustrationID, Diagram, ModifiedDate)
SELECT
    [IllustrationID],
    [Diagram],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[Illustration]
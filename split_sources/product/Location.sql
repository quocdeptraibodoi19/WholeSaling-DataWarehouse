USE Product;

Go
    DROP TABLE IF EXISTS dbo.Location;

CREATE TABLE dbo.Location (
    LocationID INT PRIMARY KEY,
    Name NVARCHAR(255),
    CostRate DECIMAL(18, 2),
    Availability INT,
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Location (
        LocationID,
        Name,
        CostRate,
        Availability,
        ModifiedDate
    )
SELECT
    [LocationID],
    [Name],
    [CostRate],
    [Availability],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[Location]
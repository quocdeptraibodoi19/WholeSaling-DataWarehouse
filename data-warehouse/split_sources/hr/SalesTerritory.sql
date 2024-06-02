Use HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.SalesTerritory;

CREATE TABLE dbo.SalesTerritory (
    TerritoryID INT PRIMARY KEY,
    Name NVARCHAR(50),
    CountryRegionCode NVARCHAR(3),
    [Group] NVARCHAR(50),
    SalesYTD MONEY,
    SalesLastYear MONEY,
    CostYTD MONEY,
    CostLastYear MONEY,
    ModifiedDate DATETIME
)
INSERT INTO
    dbo.SalesTerritory(
        [TerritoryID],
        [Name],
        [CountryRegionCode],
        [Group],
        [SalesYTD],
        [SalesLastYear],
        [CostYTD],
        [CostLastYear],
        [ModifiedDate]
    )
SELECT
    [TerritoryID],
    [Name],
    [CountryRegionCode],
    [Group],
    [SalesYTD],
    [SalesLastYear],
    [CostYTD],
    [CostLastYear],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[SalesTerritory]
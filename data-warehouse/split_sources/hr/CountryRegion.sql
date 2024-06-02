USE HumanResourceSystem;

GO
    DROP TABLE IF EXISTS dbo.CountryRegion;

CREATE TABLE dbo.CountryRegion (
    FullName NVARCHAR(255),
    Alpha2Code NVARCHAR(2),
    CountryCode NVARCHAR(3)
);

BULK
INSERT
    dbo.CountryRegion
FROM
    '/home/HolisticsCountryFormat.csv' WITH (
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2,
        -- Skip the header row if it exists
        TABLOCK
    );
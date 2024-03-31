/*
 User for constructing the BussinessEntityAddress
 Making assumption that each EmployeNationalIdNumber is unique
 */
USE WholeSaling;

GO
    DROP TABLE IF EXISTS dbo.Store;

CREATE TABLE dbo.Store (
    StoreID INT PRIMARY KEY,
    Name NVARCHAR(255),
    EmployeeNationalIDNumber NVARCHAR(15),
    Demographics NVARCHAR(MAX),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Store (
        StoreID,
        Name,
        EmployeeNationalIDNumber,
        Demographics,
        ModifiedDate
    )
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            (
                SELECT
                    NULL
            )
    ) AS StoreID,
    [Name],
    T.NationalIDNumber AS EmployeeNationalIDNumber,
    CONVERT(NVARCHAR(MAX), [Demographics]) AS Demographics,
    S.[ModifiedDate]
FROM
    [AdventureWorks2014].[Sales].[Store] S
    INNER JOIN [AdventureWorks2014].[HumanResources].[Employee] T ON S.SalesPersonID = T.BusinessEntityID;
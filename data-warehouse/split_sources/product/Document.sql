USE Product;

GO
    DROP TABLE IF EXISTS dbo.Document;

CREATE TABLE dbo.Document (
    DocumentNode hierarchyid PRIMARY KEY,
    DocumentLevel INT,
    Title NVARCHAR(255),
    Owner NVARCHAR(255),
    FolderFlag BIT,
    FileName NVARCHAR(255),
    FileExtension NVARCHAR(10),
    Revision NVARCHAR(5),
    ChangeNumber INT,
    Status INT,
    DocumentSummary NVARCHAR(MAX),
    Document VARBINARY(MAX),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.Document (
        DocumentNode,
        DocumentLevel,
        Title,
        Owner,
        FolderFlag,
        FileName,
        FileExtension,
        Revision,
        ChangeNumber,
        Status,
        DocumentSummary,
        Document,
        ModifiedDate
    )
SELECT
    [DocumentNode],
    [DocumentLevel],
    [Title],
    [Owner],
    [FolderFlag],
    [FileName],
    [FileExtension],
    [Revision],
    [ChangeNumber],
    [Status],
    [DocumentSummary],
    [Document],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[Document]
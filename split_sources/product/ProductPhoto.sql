USE Product;

GO
    DROP TABLE IF EXISTS dbo.ProductPhoto;

CREATE TABLE dbo.ProductPhoto (
    ProductPhotoID INT PRIMARY KEY,
    ThumbNailPhoto VARBINARY(MAX),
    ThumbnailPhotoFileName NVARCHAR(255),
    LargePhoto VARBINARY(MAX),
    LargePhotoFileName NVARCHAR(255),
    ModifiedDate DATETIME
);

INSERT INTO
    dbo.ProductPhoto (
        ProductPhotoID,
        ThumbNailPhoto,
        ThumbnailPhotoFileName,
        LargePhoto,
        LargePhotoFileName,
        ModifiedDate
    )
SELECT
    [ProductPhotoID],
    [ThumbNailPhoto],
    [ThumbnailPhotoFileName],
    [LargePhoto],
    [LargePhotoFileName],
    [ModifiedDate]
FROM
    [AdventureWorks2014].[Production].[ProductPhoto]
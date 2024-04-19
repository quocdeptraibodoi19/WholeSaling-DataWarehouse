{{ 
    config( materialized = 'view') 
}}

SELECT DISTINCT 
    ps.ProductSubcategoryID AS ProductSubcategoryKey,    
    ps.ProductSubcategoryID AS ProductSubcategoryAlternateKey, 
    ps.Name AS EnglishProductSubcategoryName, 
    dpc.ProductCategoryKey AS ProductCategoryKey 
FROM {{ ref("production_ProductSubcategory") }} ps 
    INNER JOIN {{ ref("dds_DimProductCategory") }} dpc 
    ON ps.ProductCategoryID = dpc.ProductCategoryAlternateKey 

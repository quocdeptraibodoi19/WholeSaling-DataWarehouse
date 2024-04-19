{{ 
    config( materialized = 'view') 
}}

SELECT DISTINCT 
    ProductCategoryID AS ProductCategoryAlternateKey, 
    `Name` AS EnglishProductCategoryName, 
FROM {{ ref("production_ProductCategory") }}
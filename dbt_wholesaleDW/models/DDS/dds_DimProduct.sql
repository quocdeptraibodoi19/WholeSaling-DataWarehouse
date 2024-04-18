{{ 
    config( materialized = 'table') 
}}

WITH CultureDescription AS (
    SELECT
        pd.Description,
        pmpdc.ProductModelID,
        trim(pmpdc.CultureID) AS CultureID,
        pd.Description
    FROM {{ ref("production_ProductModelProductDescriptionCulture") }} pmpdc
    INNER JOIN {{ ref("production_ProductDescription") }} pd
    ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID
),
EnglishDescription AS (
    SELECT *, 
        Description AS EDescription
    FROM CultureDescription WHERE CultureID = 'en'
),
FrenchDescription AS (
    SELECT *,
        Description AS FDescription
    FROM CultureDescription WHERE CultureID = 'fr'
),
ChineseDescription AS (
    SELECT *,
        Description AS CDescription
    FROM CultureDescription WHERE CultureID = 'zh-cht'
),
ArabicDescription AS (
    SELECT *,
        Description AS ADescription 
    FROM CultureDescription WHERE CultureID = 'ar'
),
HebrewDescription AS (
    SELECT *,
        Description AS HDescription 
    FROM CultureDescription WHERE CultureID = 'he'
),
ThaiDescription AS (
    SELECT *,
        Description AS TDescription 
    FROM CultureDescription WHERE CultureID = 'th'
)

SELECT
    p.ProductNumber AS ProductAlternateKey,
    p.ProductSubcategoryID AS ProductSubcategoryKey,
    p.WeightUnitMeasureCode AS WeightUnitMeasureCode,
    p.SizeUnitMeasureCode AS SizeUnitMeasureCode,
    p.Name AS EnglishProductName,
    pch.StandardCost AS StandardCost,
    p.FinishedGoodsFlag AS FinishedGoodsFlag,
    COALESCE(p.Color, 'NA') AS Color,
    p.SafetyStockLevel AS SafetyStockLevel,
    p.ReorderPoint AS ReorderPoint,
    plph.ListPrice AS ListPrice,
    p.Size AS Size,
    CASE
        WHEN p.Size IN ('38', '40') THEN "38-40 CM"
        WHEN p.Size IN ('42', '44', '46') THEN "42-46 CM"
        WHEN p.Size IN ('48', '50', '52') THEN "48-52 CM"
        WHEN p.Size IN ('54', '56', '58') THEN "54-58 CM"
        WHEN p.Size IN ('60', '62') THEN "60-62 CM"
        WHEN p.Size IN ('70') THEN "70"
        WHEN p.Size = 'L' THEN "L"
        WHEN p.Size = 'M' THEN "M"
        WHEN p.Size = 'S' THEN "S"
        WHEN p.Size = 'XL' THEN "XL"
        WHEN p.Size IS NULL THEN "NA"
    END AS SizeRange,
    CAST(p.Weight AS float) AS Weight,
    p.DaysToManufacture AS DaysToManufacture,
    p.ProductLine AS ProductLine,
    0.60 * plph.ListPrice AS DealerPrice,
    p.Class AS Class,
    p.Style AS Style,
    pm.Name AS ModelName,
    pp.LargePhoto AS LargePhoto,
    epd.EDescription AS EnglishDescription,
    fpd.FDescription AS FrenchDescription,
    cpd.CDescription AS ChineseDescription,
    apd.ADescription AS ArabicDescription,
    hpd.HDescription AS HebrewDescription,
    tpd.TDescription AS ThaiDescription,
    COALESCE(plph.StartDate, pch.StartDate, p.SellStartDate) AS StartDate,
    COALESCE(plph.EndDate, pch.EndDate, p.SellEndDate) AS EndDate,
    CASE
        WHEN COALESCE(plph.EndDate, pch.EndDate, p.SellEndDate) IS NULL THEN "Current"
        ELSE NULL
    END AS Status

FROM 
    {{ ref("production_Product") }} p

    INNER JOIN EnglishDescription epd
        ON epd.ProductModelID = p.ProductModelID

    INNER JOIN FrenchDescription fpd
        ON fpd.ProductModelID = p.ProductModelID

    INNER JOIN ChineseDescription cpd
        ON cpd.ProductModelID = p.ProductModelID

    INNER JOIN ArabicDescription apd
        ON apd.ProductModelID = p.ProductModelID

    INNER JOIN HebrewDescription hpd
        ON hpd.ProductModelID = p.ProductModelID

    INNER JOIN ThaiDescription tpd
        ON tpd.ProductModelID = p.ProductModelID
         
    LEFT OUTER JOIN {{ ref("production_ProductModel") }} pm 
        ON p.ProductModelID = pm.ProductModelID

    INNER JOIN {{ ref("production_ProductProductPhoto") }} ppp 
        ON p.ProductID = ppp.ProductID

    INNER JOIN {{ ref("production_ProductPhoto") }} pp 
        ON ppp.ProductPhotoID = pp.ProductPhotoID

    LEFT OUTER JOIN {{ ref("production_ProductCostHistory") }} pch 
        ON p.ProductID = pch.ProductID

    LEFT OUTER JOIN {{ ref("production_ProductListPriceHistory") }} plph 
        ON p.ProductID = plph.ProductID
            AND pch.StartDate = plph.StartDate
            AND COALESCE(pch.EndDate, '12-31-2020') = COALESCE(plph.EndDate, '12-31-2020')

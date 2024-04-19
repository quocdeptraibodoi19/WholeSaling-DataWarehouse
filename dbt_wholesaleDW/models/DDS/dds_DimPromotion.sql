{{ 
    config( materialized = 'view') 
}}

SELECT DISTINCT 
    so.SpecialOfferID AS PromotionAlternateKey, 
    so.Description AS EnglishPromotionName, 
    sofd.SpanishPromotionName AS SpanishPromotionName, 
    sofd.FrenchPromotionName AS FrenchPromotionName, 
    CAST(so.DiscountPct AS float) AS DiscountPct, 
    so.Type AS EnglishPromotionType, 
    so.Category AS EnglishPromotionCategory, 
    so.StartDate AS StartDate, 
    so.EndDate AS EndDate, 
    so.MinQty AS MinQty, 
    so.MaxQty AS MaxQty
FROM {{ ref("sales_SpecialOffer") }} AS so 

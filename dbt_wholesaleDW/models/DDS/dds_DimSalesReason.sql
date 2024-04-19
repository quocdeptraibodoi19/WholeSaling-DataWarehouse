{{ 
    config( materialized = 'view') 
}}

SELECT DISTINCT 
        sr.[SalesReasonID] AS [SalesReasonKey], 
        sr.[SalesReasonID] AS [SalesReasonAlternateKey], 
        sr.[Name] AS [SalesReasonName], 
        sr.[ReasonType] AS [SalesReasonReasonType] 
FROM {{ ref("sales_SalesReason") }} sr;
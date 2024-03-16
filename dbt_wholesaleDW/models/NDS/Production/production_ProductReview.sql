{{ config(materialized='view') }}

select 
    productreviewid,
    productid,   
    reviewername,      
    reviewdate,
    emailaddress,   
    rating,
    comments,
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productreview") }}
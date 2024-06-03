{{ config(materialized='view') }}

select 
    productphotoid,
    thumbnailphoto,   
    thumbnailphotofilename,      
    largephoto,
    largephotofilename,      	
    modifieddate,
    is_deleted,
    extract_date
from {{ source("production", "product_management_platform_productphoto") }}
{{ config(materialized='view') }}

select 
    productphotoid,
    thumbnailphoto,   
    thumbnailphotofilename,      
    largephoto,
    largephotofilename,      	
    modifieddate,
    is_deleted,
    date_partition
from {{ source("production", "product_management_platform_productphoto") }}
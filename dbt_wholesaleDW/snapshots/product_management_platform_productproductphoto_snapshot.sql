{% snapshot product_management_platform_productproductphoto_snapshot %}
{{    
  config( unique_key='ProductID || "-" || ProductPhotoID')   
}}  

select * from {{ source("production", "product_management_platform_productproductphoto") }}

{% endsnapshot %}
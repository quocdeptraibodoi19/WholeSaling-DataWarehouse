{% snapshot product_management_platform_productphoto_snapshot %}
{{    
  config( unique_key='ProductPhotoID' )  
}}  

select * from {{ source("production", "product_management_platform_productphoto") }}

{% endsnapshot %}
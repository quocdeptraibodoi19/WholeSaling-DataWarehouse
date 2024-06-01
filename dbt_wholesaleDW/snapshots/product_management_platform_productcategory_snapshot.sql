{% snapshot product_management_platform_productcategory_snapshot %}
{{    
  config( unique_key='ProductCategoryID' )  
}}  

select * from {{ source("production", "product_management_platform_productcategory") }}

{% endsnapshot %}
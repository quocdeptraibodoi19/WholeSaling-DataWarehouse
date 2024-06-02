{% snapshot product_management_platform_productsubcategory_snapshot %}
{{    
  config( unique_key='ProductSubcategoryID' )  
}}  

select * from {{ source("production", "product_management_platform_productsubcategory") }}

{% endsnapshot %}
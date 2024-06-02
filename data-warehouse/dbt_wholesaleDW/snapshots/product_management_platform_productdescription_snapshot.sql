{% snapshot product_management_platform_productdescription_snapshot %}
{{    
  config( unique_key='ProductDescriptionID' )  
}}  

select * from {{ source("production", "product_management_platform_productdescription") }}

{% endsnapshot %}
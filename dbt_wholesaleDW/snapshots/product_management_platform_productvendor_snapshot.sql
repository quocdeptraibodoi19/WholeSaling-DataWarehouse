{% snapshot product_management_platform_productvendor_snapshot %}
{{    
  config( unique_key='ProductID || "-" || VendorID') 
}}  

select * from {{ source("production", "product_management_platform_productvendor") }}

{% endsnapshot %}

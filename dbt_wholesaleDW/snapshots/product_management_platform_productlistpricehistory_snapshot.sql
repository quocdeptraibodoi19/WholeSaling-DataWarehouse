{% snapshot product_management_platform_productlistpricehistory_snapshot %}
{{    
  config( unique_key='ProductID || "-" || StartDate') 
}}  

select * from {{ source("production", "product_management_platform_productlistpricehistory") }}

{% endsnapshot %}
{% snapshot product_management_platform_product_snapshot %}
{{    
  config( unique_key='ProductID' )  
}}  

select * from {{ source("production", "product_management_platform_product") }}

{% endsnapshot %}


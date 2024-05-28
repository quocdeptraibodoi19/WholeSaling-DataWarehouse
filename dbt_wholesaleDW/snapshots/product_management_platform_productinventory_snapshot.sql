{% snapshot product_management_platform_productinventory_snapshot %}
{{    
  config( unique_key=['ProductID','LocationID'] )  
}}  

select * from {{ source("production", "product_management_platform_productinventory") }}

{% endsnapshot %}
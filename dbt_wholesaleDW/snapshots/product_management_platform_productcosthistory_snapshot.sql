{% snapshot product_management_platform_productcosthistory_snapshot %}
{{    
  config( unique_key=['ProductID','StartDate'] )  
}}  

select * from {{ source("production", "product_management_platform_productcosthistory") }}

{% endsnapshot %}


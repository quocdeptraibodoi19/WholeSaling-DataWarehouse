{% snapshot product_management_platform_workorder_snapshot %}
{{    
  config( unique_key='WorkOrderID' )  
}}  

select * from {{ source("production", "product_management_platform_workorder") }}

{% endsnapshot %}

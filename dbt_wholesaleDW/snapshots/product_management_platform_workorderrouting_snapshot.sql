{% snapshot product_management_platform_workorderrouting_snapshot %}
{{    
  config( unique_key='WorkOrderID || "-" || ProductID || "-" || OperationSequence')     
}}  

select * from {{ source("production", "product_management_platform_workorderrouting") }}

{% endsnapshot %}
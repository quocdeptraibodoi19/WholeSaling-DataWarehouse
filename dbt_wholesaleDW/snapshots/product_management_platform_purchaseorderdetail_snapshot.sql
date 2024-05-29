{% snapshot product_management_platform_purchaseorderdetail_snapshot %}
{{    
  config( unique_key='PurchaseOrderID || "-" || PurchaseOrderDetailID')   
}}  

select * from {{ source("production", "product_management_platform_purchaseorderdetail") }}

{% endsnapshot %}

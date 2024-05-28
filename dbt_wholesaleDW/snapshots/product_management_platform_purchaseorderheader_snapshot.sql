{% snapshot product_management_platform_purchaseorderheader_snapshot %}
{{    
  config( unique_key='PurchaseOrderID' )  
}}  

select * from {{ source("production", "product_management_platform_purchaseorderheader") }}

{% endsnapshot %}
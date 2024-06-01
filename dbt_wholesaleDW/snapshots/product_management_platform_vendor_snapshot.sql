{% snapshot product_management_platform_vendor_snapshot %}
{{    
  config( unique_key='VendorID')    
}}  

select * from {{ source("production", "product_management_platform_vendor") }}

{% endsnapshot %}
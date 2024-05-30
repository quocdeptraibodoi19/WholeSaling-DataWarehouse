{% snapshot product_management_platform_vendoraddress_snapshot %}
{{    
  config( unique_key='VendorID || "-" || AddressID || "-" || AddressTypeID')    
}}  

select * from {{ source("production", "product_management_platform_vendoraddress") }}

{% endsnapshot %}
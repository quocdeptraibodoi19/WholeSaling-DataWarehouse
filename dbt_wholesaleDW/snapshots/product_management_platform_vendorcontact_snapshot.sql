{% snapshot product_management_platform_vendorcontact_snapshot %}
{{    
  config( unique_key=['VendorID','StackHolderID'] )  
}}  

select * from {{ source("production", "product_management_platform_vendorcontact") }}

{% endsnapshot %}

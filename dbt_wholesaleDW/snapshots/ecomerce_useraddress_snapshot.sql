{% snapshot ecomerce_useraddress_snapshot %}
{{    
  config( unique_key='AddressID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_useraddress") }}

{% endsnapshot %}

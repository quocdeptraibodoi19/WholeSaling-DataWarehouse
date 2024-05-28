{% snapshot wholesale_system_storeaddress_snapshot %}
{{    
  config( unique_key=['AddressID','StoreID'] )  
}}  

select * from {{ source("wholesale", "wholesale_system_storeaddress") }}

{% endsnapshot %}
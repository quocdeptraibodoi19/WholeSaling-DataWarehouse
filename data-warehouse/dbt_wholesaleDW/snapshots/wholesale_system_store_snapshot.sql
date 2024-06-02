{% snapshot wholesale_system_store_snapshot %}
{{    
  config( unique_key='StoreID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_store") }}

{% endsnapshot %}
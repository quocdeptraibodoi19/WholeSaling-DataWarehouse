{% snapshot wholesale_system_storecontact_snapshot %}
{{    
  config( unique_key='StoreID || "-" || StackHolderID')     
}}  

select * from {{ source("wholesale", "wholesale_system_storecontact") }}

{% endsnapshot %}
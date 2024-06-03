{% snapshot wholesale_system_storerepcreditcard_snapshot %}
{{    
  config( unique_key='StoreRepID || "-" || CardNumber')     
}}  

select * from {{ source("wholesale", "wholesale_system_storerepcreditcard") }}

{% endsnapshot %}


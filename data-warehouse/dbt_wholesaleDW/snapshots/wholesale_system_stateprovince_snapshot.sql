{% snapshot wholesale_system_stateprovince_snapshot %}
{{    
  config( unique_key='StateProvinceID')    
}}  

select * from {{ source("wholesale", "wholesale_system_stateprovince") }}

{% endsnapshot %}
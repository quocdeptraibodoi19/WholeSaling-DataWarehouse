{% snapshot wholesale_system_countryregion_snapshot %}
{{    
  config( unique_key='CountryRegionCode')    
}}  

select * from {{ source("wholesale", "wholesale_system_countryregion") }}

{% endsnapshot %}
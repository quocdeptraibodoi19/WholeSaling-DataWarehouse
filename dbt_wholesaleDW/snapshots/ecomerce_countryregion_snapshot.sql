{% snapshot ecomerce_countryregion_snapshot %}
{{    
  config( unique_key='CountryRegionCode')    
}}  

select * from {{ source("ecomerce", "ecomerce_countryregion") }}

{% endsnapshot %}
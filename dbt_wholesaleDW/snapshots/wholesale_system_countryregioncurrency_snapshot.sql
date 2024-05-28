{% snapshot wholesale_system_countryregioncurrency_snapshot %}
{{    
  config( unique_key=['CountryRegionCode','CurrencyCode'] )  
}}  

select * from {{ source("wholesale", "wholesale_system_countryregioncurrency") }}

{% endsnapshot %}
{% snapshot wholesale_system_currencyrate_snapshot %}
{{    
  config( unique_key='CurrencyRateID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_currencyrate") }}

{% endsnapshot %}
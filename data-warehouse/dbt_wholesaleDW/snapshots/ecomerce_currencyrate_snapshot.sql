{% snapshot ecomerce_currencyrate_snapshot %}
{{    
  config( unique_key='CurrencyRateID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_currencyrate") }}

{% endsnapshot %}
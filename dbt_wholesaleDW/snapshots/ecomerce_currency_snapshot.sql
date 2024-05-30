{% snapshot ecomerce_currency_snapshot %}
{{    
  config( unique_key='CurrencyCode' )  
}}  

select * from {{ source("ecomerce", "ecomerce_currency") }}

{% endsnapshot %}
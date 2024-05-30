{% snapshot wholesale_system_currency_snapshot %}
{{    
  config( unique_key='CurrencyCode' )  
}}  

select * from {{ source("wholesale", "wholesale_system_currency") }}

{% endsnapshot %}
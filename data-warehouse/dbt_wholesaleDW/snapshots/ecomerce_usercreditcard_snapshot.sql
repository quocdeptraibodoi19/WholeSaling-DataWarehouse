{% snapshot ecomerce_usercreditcard_snapshot %}
{{    
  config( unique_key='UserID || "-" || CreditCardID')    
}}  

select * from {{ source("ecomerce", "ecomerce_usercreditcard") }}

{% endsnapshot %}

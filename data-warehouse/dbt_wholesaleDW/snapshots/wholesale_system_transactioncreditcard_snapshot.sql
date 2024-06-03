{% snapshot wholesale_system_transactioncreditcard_snapshot %}
{{    
  config( unique_key='CreditCardID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_transactioncreditcard") }}

{% endsnapshot %}
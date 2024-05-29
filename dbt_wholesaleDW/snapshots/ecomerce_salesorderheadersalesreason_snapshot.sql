{% snapshot ecomerce_salesorderheadersalesreason_snapshot %}
{{    
  config( unique_key='SalesOrderID || "-" || SalesReasonID')  
}}  

select * from {{ source("ecomerce", "ecomerce_salesorderheadersalesreason") }}

{% endsnapshot %}

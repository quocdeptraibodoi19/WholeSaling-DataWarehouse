{% snapshot ecomerce_salesorderheader_snapshot %}
{{    
  config( unique_key='SalesOrderID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_salesorderheader") }}

{% endsnapshot %}

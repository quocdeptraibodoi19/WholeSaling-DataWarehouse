{% snapshot wholesale_system_salesorderheader_snapshot %}
{{    
  config( unique_key='SalesOrderID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_salesorderheader") }}

{% endsnapshot %}
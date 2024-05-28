{% snapshot wholesale_system_salesorderdetail_snapshot %}
{{    
  config( unique_key='SalesOrderDetailID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_salesorderdetail") }}

{% endsnapshot %}
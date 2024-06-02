{% snapshot ecomerce_salesorderdetail_snapshot %}
{{    
  config( unique_key='SalesOrderDetailID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_salesorderdetail") }}

{% endsnapshot %}
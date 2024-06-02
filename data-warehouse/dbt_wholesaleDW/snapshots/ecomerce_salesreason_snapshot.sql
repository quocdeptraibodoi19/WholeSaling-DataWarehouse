{% snapshot ecomerce_salesreason_snapshot %}
{{    
  config( unique_key='SalesReasonID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_salesreason") }}

{% endsnapshot %}

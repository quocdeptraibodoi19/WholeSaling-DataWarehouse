{% snapshot history_salesreason %}
{{    
  config( unique_key='SalesReasonID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_salesreason") }}

{% endsnapshot %}

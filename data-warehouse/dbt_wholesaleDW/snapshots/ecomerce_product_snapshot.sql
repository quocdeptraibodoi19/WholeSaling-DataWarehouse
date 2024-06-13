{% snapshot ecomerce_product_snapshot %}
{{    
  config( unique_key='ProductID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_product") }}

{% endsnapshot %}
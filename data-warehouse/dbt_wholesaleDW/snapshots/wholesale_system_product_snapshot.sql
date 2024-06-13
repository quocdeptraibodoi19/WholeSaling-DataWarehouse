{% snapshot wholesale_system_product_snapshot %}
{{    
  config( unique_key='ProductID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_product") }}

{% endsnapshot %}
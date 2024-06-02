{% snapshot ecomerce_salesterritory_snapshot %}
{{    
  config( unique_key='TerritoryID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_salesterritory") }}

{% endsnapshot %}
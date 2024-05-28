{% snapshot wholesale_system_salesterritory_snapshot %}
{{    
  config( unique_key='TerritoryID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_salesterritory") }}

{% endsnapshot %}
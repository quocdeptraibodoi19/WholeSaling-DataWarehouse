{% snapshot wholesale_system_storecustomer_snapshot %}
{{    
  config( unique_key='CustomerID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_storecustomer") }}

{% endsnapshot %}


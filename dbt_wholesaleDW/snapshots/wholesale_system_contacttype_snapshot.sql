{% snapshot wholesale_system_contacttype_snapshot %}
{{    
  config( unique_key='ContactTypeID' )  
}}  

select * from {{ source("wholesale", "wholesale_system_contacttype") }}

{% endsnapshot %}
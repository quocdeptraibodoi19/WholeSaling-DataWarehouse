{% snapshot hr_system_addresstype_snapshot %}
{{    
  config( unique_key='AddressTypeID' )  
}}  

select * from {{ source("hr_system", "hr_system_addresstype") }}

{% endsnapshot %}
{% snapshot hr_system_salepersons_snapshot %}
{{    
  config( unique_key='EmployeeID' )  
}}  

select * from {{ source("hr_system", "hr_system_salepersons") }}

{% endsnapshot %}
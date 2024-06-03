{% snapshot hr_system_department_snapshot %}
{{    
  config( unique_key='DepartmentID' )  
}}  

select * from {{ source("hr_system", "hr_system_department") }}

{% endsnapshot %}
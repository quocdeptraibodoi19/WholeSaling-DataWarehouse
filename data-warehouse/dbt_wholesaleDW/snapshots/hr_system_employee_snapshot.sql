{% snapshot hr_system_employee_snapshot %}
{{    
  config( unique_key='EmployeeID' )  
}}  

select * from {{ source("hr_system", "hr_system_employee") }}

{% endsnapshot %}

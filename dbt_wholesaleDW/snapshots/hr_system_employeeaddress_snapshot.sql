{% snapshot hr_system_employeeaddress_snapshot %}
{{    
  config( unique_key=['EmployeeID','AddressID'] )  
}}  

select * from {{ source("hr_system", "hr_system_employeeaddress") }}

{% endsnapshot %}
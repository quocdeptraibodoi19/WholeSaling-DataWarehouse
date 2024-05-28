{% snapshot hr_system_employeedepartmenthistory_snapshot %}
{{    
  config( unique_key=['EmployeeID','DepartmentID','ShiftID','StartDate'] )  
}}  

select * from {{ source("hr_system", "hr_system_employeedepartmenthistory") }}

{% endsnapshot %}

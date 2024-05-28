{% snapshot hr_system_employeepayhistory_snapshot %}
{{    
  config( unique_key=['EmployeeID','RateChangeDate'] )  
}}  

select * from {{ source("hr_system", "hr_system_employeepayhistory") }}

{% endsnapshot %}
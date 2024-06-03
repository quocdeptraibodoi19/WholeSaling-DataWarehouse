{% snapshot hr_system_salespersonquotahistory_snapshot %}
{{    
  config( unique_key='EmployeeID || "-" || QuotaDate')  
}}  

select * from {{ source("hr_system", "hr_system_salespersonquotahistory") }}

{% endsnapshot %}
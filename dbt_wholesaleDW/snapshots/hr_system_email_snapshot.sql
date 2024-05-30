{% snapshot hr_system_email_snapshot %}
{{    
  config( unique_key='ID || "-" || Source')   
}}  

select * from {{ source("hr_system", "hr_system_email") }}

{% endsnapshot %}
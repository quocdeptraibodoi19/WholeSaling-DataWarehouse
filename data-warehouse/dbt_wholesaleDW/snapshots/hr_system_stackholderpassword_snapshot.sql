{% snapshot hr_system_stackholderpassword_snapshot %}
{{    
  config( unique_key='StackHolderID' )  
}}  

select * from {{ source("hr_system", "hr_system_stackholderpassword") }}

{% endsnapshot %}
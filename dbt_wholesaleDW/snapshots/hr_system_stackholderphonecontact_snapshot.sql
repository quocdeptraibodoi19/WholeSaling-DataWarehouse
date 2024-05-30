{% snapshot hr_system_stackholderphonecontact_snapshot %}
{{    
  config( unique_key='StackHolderID' )  
}}  

select * from {{ source("hr_system", "hr_system_stackholderphonecontact") }}

{% endsnapshot %}
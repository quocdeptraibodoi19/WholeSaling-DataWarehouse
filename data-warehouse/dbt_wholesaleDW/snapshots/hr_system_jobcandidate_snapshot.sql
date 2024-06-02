{% snapshot hr_system_jobcandidate_snapshot %}
{{    
  config( unique_key='JobCandidateID' )  
}}  

select * from {{ source("hr_system", "hr_system_jobcandidate") }}

{% endsnapshot %}
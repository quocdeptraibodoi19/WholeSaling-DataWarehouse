{% snapshot ecomerce_user_snapshot %}
{{    
  config( unique_key='UserID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_user") }}

{% endsnapshot %}

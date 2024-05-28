{% snapshot ecomerce_userpassword_snapshot %}
{{    
  config( unique_key='UserID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_userpassword") }}

{% endsnapshot %}
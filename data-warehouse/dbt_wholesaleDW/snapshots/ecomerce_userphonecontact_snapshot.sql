{% snapshot ecomerce_userphonecontact_snapshot %}
{{    
  config( unique_key='UserID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_userphonecontact") }}

{% endsnapshot %}
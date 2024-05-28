{% snapshot historecomerce_useremailaddress_snapshoty_salesreason %}
{{    
  config( unique_key='UserID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_useremailaddress") }}

{% endsnapshot %}
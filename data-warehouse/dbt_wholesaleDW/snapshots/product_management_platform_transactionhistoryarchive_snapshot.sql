{% snapshot product_management_platform_transactionhistoryarchive_snapshot %}
{{    
  config( unique_key='TransactionID' )  
}}  

select * from {{ source("production", "product_management_platform_transactionhistoryarchive") }}

{% endsnapshot %}

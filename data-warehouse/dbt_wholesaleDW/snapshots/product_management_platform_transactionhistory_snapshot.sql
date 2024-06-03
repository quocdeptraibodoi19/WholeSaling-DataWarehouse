{% snapshot product_management_platform_transactionhistory_snapshot %}
{{    
  config( unique_key='TransactionID' )  
}}  

select * from {{ source("production", "product_management_platform_transactionhistory") }}

{% endsnapshot %}

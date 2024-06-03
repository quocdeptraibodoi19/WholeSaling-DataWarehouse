{% snapshot product_management_platform_scrapreason_snapshot %}
{{    
  config( unique_key='ScrapReasonID' )  
}}  

select * from {{ source("production", "product_management_platform_scrapreason") }}

{% endsnapshot %}
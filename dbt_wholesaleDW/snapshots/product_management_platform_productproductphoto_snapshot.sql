{% snapshot product_management_platform_productproductphoto_snapshot %}
{{    
  config( unique_key=['ProductID','ProductPhotoID'] )  
}}  

select * from {{ source("production", "productproduct_management_platform_productproductphoto_management_platform_productcosthistory") }}

{% endsnapshot %}
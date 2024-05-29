{% snapshot product_management_platform_productmodelproductdescriptionculture_snapshot %}
{{    
  config( unique_key='ProductModelID || "-" || ProductDescriptionID || "-" || CultureID')   
}}  

select * from {{ source("production", "product_management_platform_productmodelproductdescriptionculture") }}

{% endsnapshot %}
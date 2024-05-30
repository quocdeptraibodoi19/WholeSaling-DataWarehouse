{% snapshot product_management_platform_billofmaterials_snapshot %}
{{    
  config( unique_key='BillOfMaterialsID' )  
}}  

select * from {{ source("production", "product_management_platform_billofmaterials") }}

{% endsnapshot %}
{% snapshot ecomerce_shoppingcartitem_snapshot %}
{{    
  config( unique_key='ShoppingCartItemID' )  
}}  

select * from {{ source("ecomerce", "ecomerce_shoppingcartitem") }}

{% endsnapshot %}
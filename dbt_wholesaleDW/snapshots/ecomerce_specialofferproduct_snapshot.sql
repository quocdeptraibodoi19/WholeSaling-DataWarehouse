{% snapshot ecomerce_specialofferproduct_snapshot %}
{{    
  config( unique_key=['SpecialOfferID','ProductID'] )  
}}  

select * from {{ source("ecomerce", "ecomerce_specialofferproduct") }}

{% endsnapshot %}

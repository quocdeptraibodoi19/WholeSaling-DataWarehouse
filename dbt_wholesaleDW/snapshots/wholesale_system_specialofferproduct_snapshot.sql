{% snapshot wholesale_system_specialofferproduct_snapshot %}
{{    
  config( unique_key='SpecialOfferID || "-" || ProductID')    
}}  

select * from {{ source("wholesale", "wholesale_system_specialofferproduct") }}

{% endsnapshot %}
{% snapshot ecomerce_specialoffer_snapshot %}
{{
  config( unique_key='SpecialOfferID')  
}}  

select * from {{ source("ecomerce", "ecomerce_specialoffer") }}

{% endsnapshot %}
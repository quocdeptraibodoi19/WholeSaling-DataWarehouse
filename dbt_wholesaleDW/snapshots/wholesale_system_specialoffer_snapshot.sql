{% snapshot wholesale_system_specialoffer_snapshot %}
{{
  config( unique_key='SpecialOfferID')  
}}  

select * from {{ source("wholesale", "wholesale_system_specialoffer") }}

{% endsnapshot %}
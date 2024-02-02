
-- Use the `ref` function to select from other models

select *
from {{ source('landingzone', 'addresstype') }}


-- Use the `ref` function to select from other models

select count(*) as count_test
from {{ source('landingzone', 'ecomerce_salesreason') }} group by salesreasonid


{{ config(materialized='table') }}

select
    distinct product_line as product_type
from {{ source("pg_sample_data", "sales_payment_line") }}

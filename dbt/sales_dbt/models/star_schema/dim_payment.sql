
{{ config(materialized='table') }}

select
    distinct payment as payment_method
from {{ source("pg_sample_data", "sales_payment_line") }}

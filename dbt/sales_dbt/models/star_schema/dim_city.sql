
{{ config(materialized='table') }}

select
    distinct city 
from {{ source("pg_sample_data", "sales_payment_line") }}

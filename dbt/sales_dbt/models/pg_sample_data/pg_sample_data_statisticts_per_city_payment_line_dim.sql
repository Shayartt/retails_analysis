
with statisticts_per_city_payment_line as (
SELECT *
FROM {{ source("pg_sample_data", "sales_payment_line") }}

),

final as (
    select * from statisticts_per_city_payment_line
    
)

select * from final


with statistics_per_customer_gender as (
SELECT *
FROM {{ source("pg_sample_data", "statistics_per_customer_gender_dim") }}

),

final as (
    select * from statistics_per_customer_gender
    
)

select * from final
where customer_type is not null 
and customer_type != ''

with statistics_per_line_quality_gender as (
SELECT *
FROM {{ source("pg_sample_data", "sales_line_quality_gender") }}

),

final as (
    select * from statistics_per_line_quality_gender
    
)

select * from final

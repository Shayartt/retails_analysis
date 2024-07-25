
with statisticts_per_city_payment_line as (
SELECT *
FROM {{ source("pg_sample_data", "sales_payment_line") }}
),

statistics_per_line_quality_gender as (
SELECT *
FROM {{ source("pg_sample_data", "sales_line_quality_gender") }}
)


select
    c.city as city_dim,
    dl.product_type,
    sum(spcp.total_sales) as total_sales,
    sum(spcp.total_number_of_orders) as total_number_of_orders,
    avg(ssplqg.average_rating) as average_rating

from {{ ref('dim_city') }} c
left join statisticts_per_city_payment_line spcp
    on c.city = spcp.city
left join {{ ref('dim_line') }} dl
    on spcp.product_line = dl.product_type
left join statistics_per_line_quality_gender ssplqg
on ssplqg.product_line = dl.product_type

group by c.city, dl.product_type

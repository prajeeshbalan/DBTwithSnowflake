{{ config(materialized='table') }}

select
    {{ date_trunc_month('updated_timestamp') }} as month_start,
    count(*) as order_count
from {{ ref('stg_orders') }}
group by 1

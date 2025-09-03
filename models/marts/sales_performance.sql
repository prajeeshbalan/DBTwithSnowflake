with last_13_months as (
    select
        date_trunc('month', sale_date) as month,
        sum(quantity * unit_price) as total_sales,
        sum(quantity) as total_quantity
    from {{ ref('fact_sales') }}
    where sale_date >= dateadd(month, -13, current_date)
    group by 1
),
high_low_months as (
    select
        month,
        total_sales,
        rank() over (order by total_sales desc) as sales_rank_desc,
        rank() over (order by total_sales asc) as sales_rank_asc
    from last_13_months
),
item_sales as (
    select
        s.item_id,
        sum(s.quantity) as total_quantity,
        sum(s.quantity * s.unit_price) as total_sales
    from {{ ref('fact_sales') }} s
    where s.sale_date >= dateadd(month, -13, current_date)
    group by s.item_id
),
high_low_items as (
    select
        item_id,
        total_quantity,
        total_sales,
        rank() over (order by total_quantity desc) as qty_rank_desc,
        rank() over (order by total_quantity asc) as qty_rank_asc
    from item_sales
)
-- Final result
select
    'monthly_sales' as metric_type,
    cast(month as date) as metric_date,
    total_sales,
    total_quantity,
    null as item_id
from last_13_months
union all
select
    'highest_sales_month' as metric_type,
    cast(month as date) as metric_date,
    total_sales,
    null as total_quantity,
    null as item_id
from high_low_months
where sales_rank_desc = 1
union all
select
    'lowest_sales_month' as metric_type,
    cast(month as date) as metric_date,
    total_sales,
    null as total_quantity,
    null as item_id
from high_low_months
where sales_rank_asc = 1
union all
select
    'highest_ordered_item' as metric_type,
    null as metric_date,
    total_sales,
    total_quantity,
    item_id
from high_low_items
where qty_rank_desc = 1
union all
select
    'lowest_ordered_item' as metric_type,
    null as metric_date,
    total_sales,
    total_quantity,
    item_id
from high_low_items
where qty_rank_asc = 1

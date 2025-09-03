{{ config(materialized = 'table') }}

select
    fo.order_id,
    fo.customer_id,
    fo.order_date,
    case
        when fo.order_status = 'Pending' then 'Pending (Status)'
        when s.order_id is null then 'Pending (No Sale)'
    end as pending_reason
from {{ ref('fact_orders') }} fo
left join {{ ref('fact_sales') }} s
    on fo.order_id = s.order_id
where fo.order_status = 'Pending'
   or s.order_id is null
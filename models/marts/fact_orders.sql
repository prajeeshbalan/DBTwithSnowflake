{{ config(materialized = 'table') }}

with base as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.payment_method,
        current_timestamp() as record_loaded_at
    from {{ ref('stg_orders') }} o
)
select * from base
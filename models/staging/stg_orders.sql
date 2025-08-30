{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge' ) }}

with source as (

    select * 
    from {{ source('rddp_raw', 'orders') }}

),
final AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        UPPER(order_status) AS order_status,              -- normalize case
        INITCAP(payment_method) AS payment_method,        -- proper case
        COALESCE(total_amount, 0) AS total_amount,        -- handle null
        discount_applied,
        INITCAP(shipping_region) AS shipping_region,      -- normalize region
        supplier_id,
        updated_timestamp
    FROM source
    {% if is_incremental() %}
        WHERE updated_timestamp > (SELECT MAX(updated_timestamp) FROM {{ this }})
    {% endif %}
)
SELECT * FROM final
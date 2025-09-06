{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_customers') }}
),

-- apply business logic
transformed as (
    select
        customer_id,
        initcap(customer_name) as customer_name,   -- format names properly
        customer_type,
        region,
        signup_date,
        is_active,
        loyalty_score,

        -- segmentation logic
        case
            when loyalty_score >= 80 then 'Platinum'
            when loyalty_score between 50 and 79 then 'Gold'
            when loyalty_score between 30 and 49 then 'Silver'
            else 'Bronze'
        end as loyalty_segment,

        email,
        phone_number,

        -- surrogate key (optional for BI)
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_type']) }} as customer_sk
    from base
)

select customer_id, customer_name from transformed

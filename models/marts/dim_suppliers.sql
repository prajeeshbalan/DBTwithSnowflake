{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_suppliers') }}
),
-- apply business logic
transformed as (
    select
        supplier_id,
        initcap(supplier_name) as supplier_name, -- clean name formatting
        supplier_type,
        country,
        rating,
        onboard_date,
        is_preferred,

        -- categorize supplier rating
        case
            when rating >= 4.5 then 'Excellent'
            when rating between 3.5 and 4.49 then 'Good'
            when rating between 2.5 and 3.49 then 'Average'
            else 'Poor'
        end as rating_category,

        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['supplier_id', 'onboard_date']) }} as supplier_sk
    from base
)
select * from transformed

{{ config(materialized='table') }}

with source as (

    select * 
    from {{ source('rddp_raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        trim(upper(customer_name))       as customer_name,   -- clean spaces + upper case
        lower(customer_type)             as customer_type,   -- normalize
        initcap(region)                  as region,          -- proper case for region
        signup_date,
        is_active,
        coalesce(loyalty_score, 0)       as loyalty_score,   -- replace nulls with 0
        lower(email)                     as email,           -- lowercase email
        regexp_replace(phone_number, '[^0-9]', '') as phone_number -- keep only digits
    from source
    where is_active = true               -- filter only active customers
)

select * from renamed

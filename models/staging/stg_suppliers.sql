{{ config(materialized='table') }}


with source as (

    select * from {{ source('rddp_raw', 'suppliers') }}

),

renamed as (

    select
        supplier_id,
        trim(initcap(supplier_name)) as supplier_name,   -- fix spacing + casing
        upper(product_category) as product_category,      -- normalize categories
        trim(initcap(city)) as city,                      -- clean city names
        upper(country) as country,                        -- standardize country
        lower(trim(email)) as email,                      -- normalize email
        regexp_replace(phone, '[^0-9]', '') as phone,     -- keep only digits
        is_active
    from source
    where is_active = true   -- filter active suppliers only
)

select * from renamed

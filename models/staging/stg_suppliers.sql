with source as (

    select * 
    from {{ source('rddp_raw', 'suppliers') }}

),

deduplicated as (

    select 
        supplier_id,
        trim(initcap(supplier_name)) as supplier_name,   -- clean names
        initcap(supplier_type) as supplier_type,         -- normalize type
        upper(country) as country,                       -- standardize country
        coalesce(rating, 0.0) as rating,                 -- handle null ratings
        onboard_date,
        is_preferred,
        row_number() over (
            partition by supplier_id 
            order by onboard_date desc
        ) as row_num
    from source

),

final as (

    select
        supplier_id,
        supplier_name,
        supplier_type,
        country,
        rating,
        onboard_date,
        is_preferred
    from deduplicated
    where row_num = 1
)

select * from final

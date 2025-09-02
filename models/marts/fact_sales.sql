{{ config(
    materialized='incremental',
    unique_key='sale_id',          
    on_schema_change='sync'
) }}

with src as (
    select
        sale_id,
        order_id,
        item_id,
        /* If you also have customer_id / supplier_id on sales, include them here as FKs (no joins) */
        -- customer_id,
        -- supplier_id,

        /* Casts for numeric stability */
        cast(quantity   as number(12,2)) as quantity,
        cast(unit_price as number(12,2)) as unit_price,

        /* Measures */
        round(quantity * unit_price, 2)                   as gross_amount,
        cast(coalesce(tax_amount, 0) as number(12,2))     as tax_amount,
        round(quantity * unit_price, 2) + coalesce(tax_amount, 0) as net_amount,

        /* Descriptive fields */
        sale_date,
        channel,
        promo_code

        /* If your table has it, keep an updated timestamp to support tighter incr. filtering */
        -- , updated_timestamp
    from {{ source('rddp_raw', 'sales') }}
)

select * from src
{% if is_incremental() %}
  /*
    Optional incremental filter to avoid scanning the full source:
    - If you have updated_timestamp on sales, prefer that.
    - Otherwise, use a rolling window on sale_date (e.g., last 90 days).
  */
  where sale_date >= (
    select coalesce(dateadd(day, -90, max(sale_date)), '1900-01-01') from {{ this }}
  )
  -- If you have updated_timestamp, replace the above with:
  -- where updated_timestamp > (select coalesce(max(updated_timestamp), '1900-01-01') from {{ this }})
{% endif %}

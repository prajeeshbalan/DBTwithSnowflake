{% snapshot customers_snapshot %}

--check strategy applied for email and phone_number
{{
    config(
        target_schema='dbt_schema',    
        unique_key='customer_id',       
        strategy='check',               
        check_cols=['email', 'phone_number'] 
    )
}}

select
    customer_id,
    email,
    phone_number,
    updated_timestamp
from {{ source('rddp_raw', 'customers') }}

{% endsnapshot %}

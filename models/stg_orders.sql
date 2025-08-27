with source as (
    select *
    from {{ source('sch060725', 'employee_data') }}
)
select 
    empno,
    upper(ename) ename
from source
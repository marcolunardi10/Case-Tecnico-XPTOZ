with total_raw as (
    select count(*) as total from {{ source('raw', 'users_info') }}
),

total_mart as (
    select coalesce(sum(total_registros), 0) as total from {{ ref('mart_dominios_email') }}
)

select r.total as total_raw, m.total as total_mart
from total_raw r
cross join total_mart m
where r.total <> m.total

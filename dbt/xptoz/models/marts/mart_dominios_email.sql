with usuarios as (

    select * from {{ ref('int_users_enriquecido') }}

)

select
    dominio_email,
    count(*) as total_registros,
    now()    as created_at,
    now()    as updated_at
from usuarios
group by dominio_email
order by total_registros desc, dominio_email

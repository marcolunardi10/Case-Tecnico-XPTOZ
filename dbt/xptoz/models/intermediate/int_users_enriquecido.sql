with usuarios as (

    select * from {{ ref('stg_users_info') }}

)

select
    id,
    nome,
    split_part(nome, ' ', 1) as primeiro_nome,
    case
        when position(' ' in nome) > 0 then regexp_replace(nome, '^.* ', '')
    end as ultimo_nome,
    email,
    split_part(email, '@', 2) as dominio_email,
    idade,
    cidade,
    created_at,
    updated_at
from usuarios

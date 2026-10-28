with origem as (

    select * from {{ source('raw', 'users_info') }}

),

padronizado as (

    select
        id,
        {{ normalizar_texto('nome') }}   as nome,
        {{ normalizar_texto('email') }}  as email,
        idade::int                       as idade,
        {{ normalizar_texto('cidade') }} as cidade
    from origem

)

select
    id,
    regexp_replace(nome, '^(SR|SRA|SRTA|DR|DRA)\.?\s+', '') as nome,
    email,
    idade,
    cidade,
    now() as created_at,
    now() as updated_at
from padronizado

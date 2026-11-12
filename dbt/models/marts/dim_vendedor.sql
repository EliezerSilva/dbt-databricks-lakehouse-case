-- Original PySpark: dropDuplicates(["seller_id"]) sobre Silver com seller_id duplicado (V004, V008).
-- Aqui: regra deterministica - prefere canal existente em dim_canal; desempate por nome_vendedor.
with vendedores as (
    select * from {{ ref('stg_vendedores') }}
),

canais as (
    select canal_id from {{ ref('dim_canal') }}
),

ranked as (
    select
        v.*,
        row_number() over (
            partition by v.seller_id
            order by
                case when c.canal_id is not null then 0 else 1 end,
                v.nome_vendedor asc nulls last,
                v.canal_id asc nulls last,
                v.regional_code, v.data_admissao, v.status
        ) as rn
    from vendedores v
    left join canais c on c.canal_id = v.canal_id
)

select
    sha2(seller_id, 256) as sk_vendedor,
    seller_id,
    nome_vendedor,
    canal_id,
    regional_code,
    data_admissao,
    status
from ranked
where rn = 1

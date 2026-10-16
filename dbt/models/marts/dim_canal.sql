-- Original PySpark: dropDuplicates(["canal_id"]) (nao deterministico).
-- Aqui: regra deterministica; para CH05 seleciona "E-commerce" (UTF8_BINARY: 'E' < 'e').
with ranked as (
    select
        canal_id,
        nome_canal,
        tipo_canal,
        ativo,
        row_number() over (
            partition by canal_id
            order by nome_canal asc nulls last, tipo_canal asc nulls last, ativo desc nulls last
        ) as rn
    from {{ ref('stg_canais') }}
)

select
    sha2(canal_id, 256) as sk_canal,
    canal_id,
    nome_canal,
    tipo_canal,
    ativo
from ranked
where rn = 1

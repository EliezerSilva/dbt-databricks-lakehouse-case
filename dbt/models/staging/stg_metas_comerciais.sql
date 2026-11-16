-- Fonte federada: cada execucao consulta o PostgreSQL externo pela conexao legado_pg.
{{ config(tags=['federacao']) }}
select
    cast(canal_id as string) as canal_id,
    cast(ano_mes as string) as ano_mes,
    meta_valor
from {{ source('legado', 'metas_comerciais') }}

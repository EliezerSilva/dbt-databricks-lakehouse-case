-- Itens orfaos do dbt x silver.tabelas_orfas do PySpark, deduplicada (o produtor grava cada item uma vez por execucao).
with dbt as (
    select chave from {{ ref('quarentena_registros') }} where regra = 'item_sem_cabecalho'
),

pyspark as (
    select distinct concat(get_json_object(payload, '$.order_id'), '-', get_json_object(payload, '$.item_seq')) as chave
    from {{ source('pyspark_quarentena', 'tabelas_orfas') }}
    where source_table = 'fato_pedidos'
)

select 'so_no_dbt' as lado, chave from (select chave from dbt except select chave from pyspark)
union all
select 'so_no_pyspark' as lado, chave from (select chave from pyspark except select chave from dbt)

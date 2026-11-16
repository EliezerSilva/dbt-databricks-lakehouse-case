-- Quarentena do consumidor: uma linha por (entidade, regra, chave), independente de quantas vezes o produtor rodou.
-- consumidor_dbt: violacoes detectadas pelo dbt (itens sem cabecalho).
-- produtor_pyspark: registros barrados pelo PySpark antes da silver; so visiveis via silver.tabelas_invalidas.
with itens_orfaos as (
    select
        'pedidos_itens' as entidade,
        'item_sem_cabecalho' as regra,
        'consumidor_dbt' as origem,
        concat(order_id, '-', cast(item_seq as string)) as chave,
        order_id as chave_negocio,
        to_json(struct(order_id, item_seq, product_code, quantity, unit_price, total_item, item_status)) as payload,
        processing_timestamp as processado_em
    from {{ ref('itens_sem_cabecalho') }}
),

invalidos_produtor as (
    select
        'regioes' as entidade,
        motivo as regra,
        'produtor_pyspark' as origem,
        get_json_object(payload, '$.regional_code') as chave,
        get_json_object(payload, '$.regional_code') as chave_negocio,
        max_by(payload, processing_timestamp) as payload,
        max(processing_timestamp) as processado_em
    from {{ source('pyspark_quarentena', 'tabelas_invalidas') }}
    where source_table = 'silver_regioes'
    group by motivo, get_json_object(payload, '$.regional_code')
)

select *, {{ var('contrato_versao', 1) }} as contrato_versao from itens_orfaos
union all
select *, {{ var('contrato_versao', 1) }} as contrato_versao from invalidos_produtor

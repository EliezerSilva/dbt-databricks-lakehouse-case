{{ config(materialized='view') }}
-- Itens cujo order_id nao existe no cabecalho. No PySpark, gravados em append em silver.tabelas_orfas
-- durante a construcao da fato; aqui, view idempotente sem efeito colateral.
select i.*
from {{ ref('stg_pedidos_itens') }} i
left anti join {{ ref('stg_pedidos_cabecalho') }} c on c.order_id = i.order_id

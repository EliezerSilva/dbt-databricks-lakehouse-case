{%- set simular = var('simular_cabecalho_tardio', false) -%}
{%- if simular and target.schema == 'dbt' -%}
    {{ exceptions.raise_compiler_error("simular_cabecalho_tardio nao pode rodar no schema dbt (ambiente real); use scripts/simular_cabecalho_tardio.sh") }}
{%- endif -%}
select
    order_id,
    customer_code,
    seller_id,
    order_date,
    promised_date,
    status_order,
    gross_amount,
    discount_amount,
    net_amount,
    last_update,
    order_source,
    order_priority,
    processing_timestamp
from {{ source('silver', 'silver_pedidos_cabecalho') }}
{%- if simular %}
-- Simulacao: cabecalho que chega depois dos itens (ver seeds/cabecalhos_tardios.csv).
union all
select
    order_id,
    customer_code,
    seller_id,
    order_date,
    promised_date,
    status_order,
    gross_amount,
    discount_amount,
    net_amount,
    last_update,
    order_source,
    order_priority,
    processing_timestamp
from {{ ref('cabecalhos_tardios') }}
{%- endif %}

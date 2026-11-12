-- Mesma regra do PySpark: datas de pedidos e entregas, de (min - 30) a (max + 30) dias.
with datas as (
    select order_date as dt from {{ ref('stg_pedidos_cabecalho') }}
    union all
    select promised_date from {{ ref('stg_pedidos_cabecalho') }}
    union all
    select cast(shipped_at as date) from {{ ref('stg_logistica') }}
    union all
    select cast(delivered_at as date) from {{ ref('stg_logistica') }}
),

limites as (
    select date_sub(min(dt), 30) as inicio, date_add(max(dt), 30) as fim
    from datas
    where dt is not null
),

calendario as (
    select explode(sequence(inicio, fim, interval 1 day)) as data
    from limites
),

base as (
    select data, case when dayofweek(data) = 1 then 7 else dayofweek(data) - 1 end as dia_semana_iso
    from calendario
)

select
    data,
    cast(date_format(data, 'yyyyMMdd') as int) as sk_data,
    year(data) as ano,
    month(data) as mes,
    dayofmonth(data) as dia,
    quarter(data) as trimestre,
    weekofyear(data) as semana_ano,
    element_at(array('Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho',
                     'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'), month(data)) as nome_mes,
    dia_semana_iso,
    element_at(array('Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'), dia_semana_iso) as nome_dia_semana,
    case when dia_semana_iso in (6, 7) then 1 else 0 end as flag_fim_semana,
    date_format(data, 'yyyy-MM') as ano_mes
from base

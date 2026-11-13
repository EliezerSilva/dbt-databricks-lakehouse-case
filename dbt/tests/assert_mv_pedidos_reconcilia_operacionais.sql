-- Compara apenas as metricas que ja estavam no grao correto no PySpark (pedidos distintos).
with mv as (
    select ano_mes, regiao, canal, measure(pedidos) as p, measure(pedidos_com_cancelamento) as c,
           measure(pedidos_atrasados) as a, measure(taxa_cancelamento_pct) as tc, measure(taxa_atraso_pct) as ta
    from {{ ref('mv_pedidos') }} group by ano_mes, regiao, canal
),
py as (
    select ano_mes, coalesce(nome_regiao, 'Sem região') as regiao, coalesce(nome_canal, 'Sem canal') as canal,
           total_pedidos as p, total_cancelados as c, total_atrasados as a,
           taxa_cancelamento_pct as tc, taxa_atraso_pct as ta
    from {{ source('pyspark_gold', 'metricas_operacionais') }}
)
select coalesce(mv.ano_mes, py.ano_mes) as ano_mes, coalesce(mv.regiao, py.regiao) as regiao,
       coalesce(mv.canal, py.canal) as canal, mv.p, py.p, mv.c, py.c, mv.a, py.a, mv.tc, py.tc, mv.ta, py.ta
from mv full outer join py on mv.ano_mes <=> py.ano_mes and mv.regiao = py.regiao and mv.canal = py.canal
where not (mv.p <=> py.p and mv.c <=> py.c and mv.a <=> py.a
           and round(mv.tc, 2) <=> round(py.tc, 2) and round(mv.ta, 2) <=> round(py.ta, 2))

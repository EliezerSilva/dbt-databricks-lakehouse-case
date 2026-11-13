with mv as (
    select ano_mes, measure(receita_bruta) as rb, measure(receita_liquida) as rl, measure(pedidos) as p
    from {{ ref('mv_vendas') }} group by ano_mes
),
py as (
    select ano_mes, receita_bruta as rb, receita_liquida as rl, total_pedidos as p
    from {{ source('pyspark_gold', 'metricas_por_periodo') }}
)
select coalesce(mv.ano_mes, py.ano_mes) as ano_mes, mv.rb, py.rb, mv.rl, py.rl, mv.p, py.p
from mv full outer join py on mv.ano_mes <=> py.ano_mes
where not (round(mv.rb, 2) <=> round(py.rb, 2) and round(mv.rl, 2) <=> round(py.rl, 2) and mv.p <=> py.p)

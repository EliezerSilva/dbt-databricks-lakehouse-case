with mv as (
    select regiao, measure(receita_bruta) as rb, measure(receita_liquida) as rl, measure(pedidos) as p, measure(clientes) as c
    from {{ ref('mv_vendas') }} group by regiao
),
py as (
    select coalesce(nome_regiao, 'Sem região') as regiao, receita_bruta as rb, receita_liquida as rl, total_pedidos as p, total_clientes_ativos as c
    from {{ source('pyspark_gold', 'metricas_por_regiao') }}
)
select coalesce(mv.regiao, py.regiao) as regiao, mv.rb, py.rb, mv.rl, py.rl, mv.p, py.p, mv.c, py.c
from mv full outer join py on mv.regiao = py.regiao
where not (round(mv.rb, 2) <=> round(py.rb, 2) and round(mv.rl, 2) <=> round(py.rl, 2) and mv.p <=> py.p and mv.c <=> py.c)

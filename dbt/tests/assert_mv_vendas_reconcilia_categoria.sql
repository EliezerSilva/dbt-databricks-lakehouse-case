with mv as (
    select categoria, subcategoria, measure(receita_bruta) as rb, measure(receita_liquida) as rl,
           measure(quantidade) as q, measure(pedidos) as p, measure(preco_medio_vendido) as pm
    from {{ ref('mv_vendas') }} group by categoria, subcategoria
),
py as (
    select categoria, subcategoria, receita_bruta as rb, receita_liquida as rl, total_quantidade as q,
           total_pedidos as p, preco_medio_vendido as pm
    from {{ source('pyspark_gold', 'metricas_por_categoria') }}
)
select coalesce(mv.categoria, py.categoria) as categoria, coalesce(mv.subcategoria, py.subcategoria) as subcategoria,
       mv.rb, py.rb, mv.q, py.q, mv.p, py.p, mv.pm, py.pm
from mv full outer join py on mv.categoria <=> py.categoria and mv.subcategoria <=> py.subcategoria
where not (round(mv.rb, 2) <=> round(py.rb, 2) and round(mv.rl, 2) <=> round(py.rl, 2)
           and round(mv.q, 2) <=> round(py.q, 2) and mv.p <=> py.p and round(mv.pm, 2) <=> round(py.pm, 2))

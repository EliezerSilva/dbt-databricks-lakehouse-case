-- Meta (sistema legado federado) x receita liquida realizada (fato_pedidos), por canal e mes.
{{ config(tags=['federacao']) }}
with realizado as (
    select
        c.canal_id,
        date_format(f.data_pedido, 'yyyy-MM') as ano_mes,
        sum(f.receita_liquida_item) as realizado
    from {{ ref('fato_pedidos') }} f
    join {{ ref('dim_canal') }} c on c.sk_canal = f.sk_canal
    group by 1, 2
)

select
    m.canal_id,
    m.ano_mes,
    m.meta_valor,
    round(coalesce(r.realizado, 0), 2) as realizado,
    round(coalesce(r.realizado, 0) / m.meta_valor * 100, 1) as atingimento_pct
from {{ ref('stg_metas_comerciais') }} m
left join realizado r on r.canal_id = m.canal_id and r.ano_mes = m.ano_mes

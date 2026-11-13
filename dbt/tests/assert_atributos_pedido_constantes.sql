-- Premissa do fct_pedidos: atributos de pedido nao variam entre os itens do mesmo pedido.
select order_id
from {{ ref('fato_pedidos') }}
group by order_id
having count(distinct coalesce(valor_liquido_pedido, -1)) > 1
    or count(distinct coalesce(valor_desconto_pedido, -1)) > 1
    or count(distinct coalesce(qtd_ocorrencias, -1)) > 1
    or count(distinct coalesce(tempo_entrega_dias, -1)) > 1
    or count(distinct flag_atrasado) > 1
    or count(distinct coalesce(sk_canal, '-')) > 1
    or count(distinct coalesce(sk_regiao, '-')) > 1
    or count(distinct coalesce(sk_cliente, '-')) > 1
    or count(distinct status_order) > 1

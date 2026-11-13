-- Grao: 1 linha por pedido (com cabecalho e ao menos um item na fato).
-- Atributos de pedido sao constantes entre os itens (ver assert_atributos_pedido_constantes).
select
    order_id,
    max(sk_cliente) as sk_cliente,
    max(sk_canal) as sk_canal,
    max(sk_regiao) as sk_regiao,
    max(sk_vendedor) as sk_vendedor,
    max(sk_data_pedido) as sk_data_pedido,
    max(status_order) as status_order,
    max(status_entrega) as status_entrega,
    max(valor_bruto_pedido) as valor_bruto_pedido,
    max(valor_desconto_pedido) as valor_desconto_pedido,
    max(valor_liquido_pedido) as valor_liquido_pedido,
    max(custo_entrega) as custo_entrega,
    max(tempo_entrega_dias) as tempo_entrega_dias,
    max(dias_atraso) as dias_atraso,
    max(flag_atrasado) as flag_atrasado,
    max(flag_tem_ocorrencia) as flag_tem_ocorrencia,
    max(qtd_ocorrencias) as qtd_ocorrencias,
    max(flag_cancel_request) as flag_cancel_request,
    max(flag_devolucao) as flag_devolucao,
    max(flag_cancelado) as flag_cancelado_algum,
    max(case when upper(status_order) = 'CANCELADO' then 1 else 0 end) as flag_pedido_cancelado,
    count(*) as qtd_itens,
    sum(valor_item) as valor_itens,
    sum(receita_liquida_item) as receita_liquida,
    max(data_pedido) as data_pedido
from {{ ref('fato_pedidos') }}
group by order_id

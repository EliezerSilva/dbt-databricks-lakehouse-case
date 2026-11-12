-- Grao: 1 linha por item de pedido com cabecalho. Itens sem cabecalho: ver itens_sem_cabecalho.
-- receita_liquida_item: rateio proporcional do desconto do pedido pelo valor do item (regra do PySpark).
with pedidos as (
    select
        i.order_id, i.item_seq, i.product_code, i.quantity, i.unit_price, i.total_item, i.item_status,
        c.customer_code, c.seller_id, c.order_date, c.promised_date, c.status_order,
        c.gross_amount, c.discount_amount, c.net_amount, c.order_source
    from {{ ref('stg_pedidos_itens') }} i
    left join {{ ref('stg_pedidos_cabecalho') }} c on c.order_id = i.order_id
    where c.status_order is not null
),

ocorrencias as (
    select
        order_id,
        count(ticket_id) as qtd_ocorrencias,
        max(case when event_type = 'cancel_request' then 1 else 0 end) as flag_cancel_request,
        max(case when event_type in ('refund', 'troca') then 1 else 0 end) as flag_devolucao,
        1 as flag_tem_ocorrencia
    from {{ ref('stg_atendimento_ocorrencias') }}
    group by order_id
),

logistica as (
    select
        order_ref, delivery_status, shipped_at, delivered_at, cost as custo_entrega,
        flag_atrasado, dias_atraso, tempo_entrega_dias
    from (
        select *, row_number() over (partition by order_ref order by delivered_at desc nulls last, delivery_id) as rn
        from {{ ref('stg_logistica') }}
    )
    where rn = 1
),

base as (
    select
        p.*,
        l.delivery_status, l.shipped_at, l.delivered_at, l.custo_entrega, l.dias_atraso, l.tempo_entrega_dias,
        coalesce(o.flag_tem_ocorrencia, 0) as flag_tem_ocorrencia,
        coalesce(o.qtd_ocorrencias, 0) as qtd_ocorrencias,
        coalesce(o.flag_cancel_request, 0) as flag_cancel_request,
        coalesce(o.flag_devolucao, 0) as flag_devolucao,
        coalesce(l.flag_atrasado, 0) as flag_atrasado,
        case when upper(p.status_order) = 'CANCELADO' or p.item_status = 'CANCELADO' then 1 else 0 end as flag_cancelado
    from pedidos p
    left join logistica l on p.order_id = l.order_ref
    left join ocorrencias o on o.order_id = p.order_id
)

select
    b.order_id,
    b.item_seq,
    cl.sk_cliente,
    pr.sk_produto,
    cn.sk_canal,
    rg.sk_regiao,
    vd.sk_vendedor,
    dp.sk_data as sk_data_pedido,
    dpr.sk_data as sk_data_prevista,
    b.quantity as quantidade,
    b.unit_price as valor_unitario,
    b.total_item as valor_item,
    case
        when b.gross_amount is not null and b.gross_amount > 0
            then round(b.total_item - (coalesce(b.discount_amount, 0) * b.total_item / b.gross_amount), 2)
        else b.total_item
    end as receita_liquida_item,
    b.gross_amount as valor_bruto_pedido,
    b.discount_amount as valor_desconto_pedido,
    b.net_amount as valor_liquido_pedido,
    b.custo_entrega,
    b.tempo_entrega_dias,
    b.status_order,
    b.item_status,
    b.delivery_status as status_entrega,
    b.flag_cancelado,
    b.flag_atrasado,
    b.dias_atraso,
    b.flag_tem_ocorrencia,
    b.qtd_ocorrencias,
    b.flag_cancel_request,
    b.flag_devolucao,
    b.order_date as data_pedido,
    b.promised_date as data_prevista_entrega,
    b.shipped_at as data_envio,
    b.delivered_at as data_entrega_real,
    b.order_source,
    current_timestamp() as processing_timestamp
from base b
left join {{ ref('dim_cliente') }} cl on b.customer_code = cl.customer_id
left join {{ ref('dim_produto') }} pr on b.product_code = pr.product_id
left join {{ ref('dim_vendedor') }} vd on b.seller_id = vd.seller_id
left join {{ ref('dim_canal') }} cn on vd.canal_id = cn.canal_id
left join {{ ref('dim_regiao') }} rg on vd.regional_code = rg.regional_code
left join {{ ref('dim_data') }} dp on b.order_date = dp.data
left join {{ ref('dim_data') }} dpr on b.promised_date = dpr.data

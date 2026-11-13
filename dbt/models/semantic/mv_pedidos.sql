{{ config(materialized='metric_view') }}
version: 1.1
comment: "Pedidos no grao de pedido. Atributos de pedido agregados uma vez por pedido (corrige a dupla contagem das metricas PySpark)."
source: {{ ref('fct_pedidos') | replace('`', '') }}
joins:
  - name: d_calendario
    source: {{ ref('dim_data') | replace('`', '') }}
    on: source.sk_data_pedido = d_calendario.sk_data
  - name: d_canal
    source: {{ ref('dim_canal') | replace('`', '') }}
    on: source.sk_canal = d_canal.sk_canal
  - name: d_regiao
    source: {{ ref('dim_regiao') | replace('`', '') }}
    on: source.sk_regiao = d_regiao.sk_regiao
  - name: d_cliente
    source: {{ ref('dim_cliente') | replace('`', '') }}
    on: source.sk_cliente = d_cliente.sk_cliente
dimensions:
  - name: ano_mes
    expr: d_calendario.ano_mes
  - name: ano
    expr: d_calendario.ano
  - name: canal
    expr: coalesce(d_canal.nome_canal, 'Sem canal')
  - name: regiao
    expr: coalesce(d_regiao.nome_regiao, 'Sem região')
  - name: segmento
    expr: coalesce(d_cliente.segmento, 'Não informado')
  - name: status_pedido
    expr: source.status_order
measures:
  - name: pedidos
    expr: COUNT(1)
  - name: pedidos_ativos
    expr: COUNT(1) FILTER (WHERE source.flag_pedido_cancelado = 0)
  - name: pedidos_com_cancelamento
    expr: COUNT(1) FILTER (WHERE source.flag_cancelado_algum = 1)
  - name: taxa_cancelamento_pct
    expr: ROUND(COUNT(1) FILTER (WHERE source.flag_cancelado_algum = 1) * 100.0 / COUNT(1), 2)
  - name: pedidos_atrasados
    expr: COUNT(1) FILTER (WHERE source.flag_atrasado = 1)
  - name: taxa_atraso_pct
    expr: ROUND(COUNT(1) FILTER (WHERE source.flag_atrasado = 1) * 100.0 / COUNT(1), 2)
  - name: ticket_medio
    expr: AVG(source.valor_liquido_pedido) FILTER (WHERE source.flag_pedido_cancelado = 0)
  - name: descontos
    expr: SUM(source.valor_desconto_pedido) FILTER (WHERE source.flag_pedido_cancelado = 0)
  - name: ocorrencias
    expr: SUM(source.qtd_ocorrencias)
  - name: pedidos_com_ocorrencia
    expr: COUNT(1) FILTER (WHERE source.flag_tem_ocorrencia = 1)
  - name: pedidos_com_devolucao
    expr: COUNT(1) FILTER (WHERE source.flag_devolucao = 1)
  - name: pedidos_com_pedido_de_cancelamento
    expr: COUNT(1) FILTER (WHERE source.flag_cancel_request = 1)
  - name: tempo_medio_entrega_dias
    expr: AVG(source.tempo_entrega_dias)
  - name: media_dias_atraso
    expr: AVG(source.dias_atraso) FILTER (WHERE source.dias_atraso > 0)
  - name: custo_frete
    expr: SUM(try_cast(source.custo_entrega AS DOUBLE))

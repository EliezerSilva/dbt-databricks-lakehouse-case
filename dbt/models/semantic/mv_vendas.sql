{{ config(materialized='metric_view') }}
version: 1.1
comment: "Vendas no grao de item de pedido. Medidas excluem itens cancelados (regra do PySpark)."
source: {{ ref('fato_pedidos') | replace('`', '') }}
joins:
  - name: d_calendario
    source: {{ ref('dim_data') | replace('`', '') }}
    on: source.sk_data_pedido = d_calendario.sk_data
  - name: d_produto
    source: {{ ref('dim_produto') | replace('`', '') }}
    on: source.sk_produto = d_produto.sk_produto
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
  - name: trimestre
    expr: d_calendario.trimestre
  - name: categoria
    expr: d_produto.categoria
  - name: subcategoria
    expr: d_produto.subcategoria
  - name: canal
    expr: coalesce(d_canal.nome_canal, 'Sem canal')
  - name: tipo_canal
    expr: coalesce(d_canal.tipo_canal, 'Sem canal')
  - name: regiao
    expr: coalesce(d_regiao.nome_regiao, 'Sem região')
  - name: segmento
    expr: coalesce(d_cliente.segmento, 'Não informado')
  - name: status_pedido
    expr: source.status_order
measures:
  - name: receita_bruta
    expr: SUM(source.valor_item) FILTER (WHERE source.flag_cancelado = 0)
  - name: receita_liquida
    expr: SUM(source.receita_liquida_item) FILTER (WHERE source.flag_cancelado = 0)
  - name: quantidade
    expr: SUM(source.quantidade) FILTER (WHERE source.flag_cancelado = 0)
  - name: pedidos
    expr: COUNT(DISTINCT source.order_id) FILTER (WHERE source.flag_cancelado = 0)
  - name: clientes
    expr: COUNT(DISTINCT source.sk_cliente) FILTER (WHERE source.flag_cancelado = 0)
  - name: preco_medio_vendido
    expr: AVG(source.valor_unitario) FILTER (WHERE source.flag_cancelado = 0)

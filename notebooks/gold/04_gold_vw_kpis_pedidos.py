# Databricks notebook source
# MAGIC %run ../00_setup

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_pedidos_analytics AS
    SELECT
        f.order_id,
        f.item_seq,

        f.data_pedido,
        f.data_prevista_entrega,
        f.data_envio,

        d.ano,
        d.mes,
        d.trimestre,
        d.ano_mes,
        d.nome_mes,
        d.flag_fim_semana,

        cl.customer_id,
        cl.nome_cliente,
        cl.segmento,
        cl.porte,
        cl.cidade,
        cl.estado,

        pr.product_id,
        pr.nome_produto,
        pr.categoria,
        pr.subcategoria,
        pr.familia,
        pr.preco_lista,

        cn.canal_id,
        cn.nome_canal,
        cn.tipo_canal,

        rg.regional_code,
        rg.nome_regiao,
        rg.estado_sede,

        vd.seller_id,
        vd.nome_vendedor,

        f.quantidade,
        f.valor_unitario,
        f.valor_item,
        f.receita_liquida_item,

        f.valor_bruto_pedido,
        f.valor_desconto_pedido,
        f.valor_liquido_pedido,
        f.custo_entrega,
        f.tempo_entrega_dias,

        f.status_order,
        f.item_status,
        f.status_entrega,
        f.flag_atrasado,
        f.dias_atraso,
        f.flag_tem_ocorrencia,
        f.qtd_ocorrencias,
        f.flag_cancel_request,
        f.flag_devolucao,
        f.order_source

    FROM {GOLD_SCHEMA}.fato_pedidos f
    LEFT JOIN {GOLD_SCHEMA}.dim_data     d  ON f.sk_data_pedido = d.sk_data
    LEFT JOIN {GOLD_SCHEMA}.dim_cliente  cl ON f.sk_cliente     = cl.sk_cliente
    LEFT JOIN {GOLD_SCHEMA}.dim_produto  pr ON f.sk_produto     = pr.sk_produto
    LEFT JOIN {GOLD_SCHEMA}.dim_canal    cn ON f.sk_canal       = cn.sk_canal
    LEFT JOIN {GOLD_SCHEMA}.dim_regiao   rg ON f.sk_regiao      = rg.sk_regiao
    LEFT JOIN {GOLD_SCHEMA}.dim_vendedor vd ON f.sk_vendedor    = vd.sk_vendedor

    WHERE f.flag_cancelado = 0
""")

print("gold.vw_pedidos_analytics criada")

# COMMAND ----------

n = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {GOLD_SCHEMA}.vw_pedidos_analytics
""").first()["n"]

print(f"vw_pedidos_analytics: {n} rows ativos")

checks = spark.sql(f"""
    SELECT
        COUNT(CASE WHEN nome_cliente IS NULL THEN 1 END) AS sem_cliente,
        COUNT(CASE WHEN nome_produto IS NULL THEN 1 END) AS sem_produto,
        COUNT(CASE WHEN nome_canal IS NULL THEN 1 END)   AS sem_canal,
        COUNT(CASE WHEN nome_regiao IS NULL THEN 1 END)  AS sem_regiao,
        COUNT(CASE WHEN ano IS NULL THEN 1 END)          AS sem_data
    FROM {GOLD_SCHEMA}.vw_pedidos_analytics
""").first()

print(f"""
Dimensões sem correspondência:
  sem_cliente : {checks['sem_cliente']}
  sem_produto : {checks['sem_produto']}
  sem_canal   : {checks['sem_canal']}
  sem_regiao  : {checks['sem_regiao']}
  sem_data    : {checks['sem_data']}
""")
# Databricks notebook source
# MAGIC %run ../00_setup

# COMMAND ----------

# DBTITLE 1,Validation — catalogs, schemas and storage objects
print("Available catalogs:")
spark.sql("SHOW CATALOGS").show()

print("\nSchemas in workspace catalog:")
spark.sql("SHOW SCHEMAS IN workspace").show()

print("\nAvailable volumes in lakehouse_case schema:")
spark.sql("SHOW VOLUMES IN workspace.lakehouse_case").show()

print("\nBronze tables:")
spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").show(truncate=False)

print("\nSilver tables:")
spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}").show(truncate=False)

print("\nGold tables:")
spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}").show(truncate=False)

print("\nControl tables:")
spark.sql(f"SHOW TABLES IN {CONTROL_SCHEMA}").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,table ingestion_log
display(spark.sql(f"""
SELECT COUNT(*)
FROM {BRONZE_SCHEMA}.bronze_erp_pedidos_cabecalho;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    source_file,
    target_table,
    records_loaded,
    status,
    bronze_load_timestamp
FROM {CONTROL_SCHEMA}.ingestion_log
ORDER BY bronze_load_timestamp DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SHOW TABLES IN {SILVER_SCHEMA};
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*)
FROM {SILVER_SCHEMA}.silver_pedidos_cabecalho;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    order_id,
    order_date,
    promised_date
FROM {SILVER_SCHEMA}.silver_pedidos_cabecalho
LIMIT 20;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    status_order,
    COUNT(*) AS total
FROM {SILVER_SCHEMA}.silver_pedidos_cabecalho
GROUP BY status_order
ORDER BY total DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    source_table AS table_name,
    check_name AS quality_rule,
    records_affected,
    message AS details
FROM {SILVER_SCHEMA}.quality_log
ORDER BY processing_timestamp DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    order_source,
    order_priority
FROM {SILVER_SCHEMA}.silver_pedidos_cabecalho
LIMIT 20;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*)
FROM {SILVER_SCHEMA}.silver_pedidos_itens;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    order_id,
    item_seq,
    COUNT(*) AS total
FROM {SILVER_SCHEMA}.silver_pedidos_itens
GROUP BY order_id, item_seq
HAVING COUNT(*) > 1;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    item_status,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_pedidos_itens
GROUP BY item_status
ORDER BY 2 DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    source_table,
    check_name,
    records_affected,
    message
FROM {SILVER_SCHEMA}.quality_log
WHERE source_table = 'silver_pedidos_itens'
ORDER BY processing_timestamp DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SHOW TABLES IN {SILVER_SCHEMA};
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {SILVER_SCHEMA}.silver_regioes;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {SILVER_SCHEMA}.silver_canais;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {SILVER_SCHEMA}.silver_vendedores;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {SILVER_SCHEMA}.silver_clientes;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    estado,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_clientes
GROUP BY estado
ORDER BY 2 DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    delivery_status,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_logistica
GROUP BY delivery_status
ORDER BY 2 DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    flag_atrasado,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_logistica
GROUP BY flag_atrasado;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    severity,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_atendimento_ocorrencias
GROUP BY severity
ORDER BY 2 DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    source_table,
    check_name,
    records_affected,
    message
FROM {SILVER_SCHEMA}.quality_log
ORDER BY processing_timestamp DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SHOW TABLES IN {GOLD_SCHEMA};
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) total,
    COUNT(DISTINCT sk_cliente) sk_distintas
FROM {GOLD_SCHEMA}.dim_cliente;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) total,
    COUNT(DISTINCT sk_produto) sk_distintas
FROM {GOLD_SCHEMA}.dim_produto;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) total,
    COUNT(DISTINCT sk_data) sk_distintas
FROM {GOLD_SCHEMA}.dim_data;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    data,
    nome_mes,
    nome_dia_semana,
    flag_fim_semana
FROM {GOLD_SCHEMA}.dim_data
ORDER BY data
LIMIT 20;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    MIN(data),
    MAX(data)
FROM {GOLD_SCHEMA}.dim_data;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*)
FROM {GOLD_SCHEMA}.fato_pedidos;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) total,
    COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
FROM {GOLD_SCHEMA}.fato_pedidos;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    flag_atrasado,
    COUNT(*)
FROM {GOLD_SCHEMA}.fato_pedidos
GROUP BY flag_atrasado;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    flag_cancelado,
    COUNT(*)
FROM {GOLD_SCHEMA}.fato_pedidos
GROUP BY flag_cancelado;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    valor_item,
    valor_desconto_pedido,
    receita_liquida_item
FROM {GOLD_SCHEMA}.fato_pedidos
LIMIT 20;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    order_id,
    item_seq,
    COUNT(*) qtd
FROM {GOLD_SCHEMA}.fato_pedidos
GROUP BY order_id, item_seq
HAVING COUNT(*) > 1
ORDER BY qtd DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    seller_id,
    COUNT(*)
FROM {GOLD_SCHEMA}.dim_vendedor
GROUP BY seller_id
HAVING COUNT(*) > 1;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    order_ref,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_logistica
WHERE order_ref IN ('O00081', 'O00011', 'O00121')
GROUP BY order_ref;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT *
FROM {SILVER_SCHEMA}.silver_logistica
WHERE order_ref IN ('O00081', 'O00011', 'O00121')
ORDER BY order_ref, delivered_at;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    order_id,
    item_seq,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_pedidos_itens
WHERE order_id IN ('O00081', 'O00011', 'O00121')
GROUP BY order_id, item_seq
HAVING COUNT(*) > 1;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) total,
    COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
FROM {GOLD_SCHEMA}.fato_pedidos;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    order_id,
    COUNT(*)
FROM {SILVER_SCHEMA}.silver_pedidos_cabecalho
GROUP BY order_id
HAVING COUNT(*) > 1;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COUNT(*) total,
    COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
FROM {GOLD_SCHEMA}.fato_pedidos;
"""))

# COMMAND ----------

display(spark.sql(f"""
SHOW TABLES IN {GOLD_SCHEMA};
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {GOLD_SCHEMA}.metricas_por_periodo;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {GOLD_SCHEMA}.metricas_por_regiao;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {GOLD_SCHEMA}.metricas_por_canal;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {GOLD_SCHEMA}.metricas_por_categoria;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(*) FROM {GOLD_SCHEMA}.metricas_operacionais;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    ano_mes,
    receita_bruta,
    receita_liquida,
    total_descontos,
    ticket_medio
FROM {GOLD_SCHEMA}.metricas_por_periodo
ORDER BY ano_mes;
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
    ano_mes,
    nome_regiao,
    nome_canal,
    taxa_cancelamento_pct,
    taxa_atraso_pct
FROM {GOLD_SCHEMA}.metricas_operacionais
ORDER BY ano_mes
LIMIT 30;
"""))

# COMMAND ----------

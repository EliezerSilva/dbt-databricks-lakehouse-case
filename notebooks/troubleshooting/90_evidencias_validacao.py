# Databricks notebook source
# MAGIC %run ../00_setup

# COMMAND ----------

display(spark.sql(f"""
--01 — TABELAS GOLD
--Evidência da camada analítica final

SHOW TABLES IN {GOLD_SCHEMA};
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 02 — QUALITY LOG
-- Evidência principal de governança e tratamento
SELECT
    source_table,
    check_name,
    records_affected,
    message,
    processing_timestamp
FROM {SILVER_SCHEMA}.quality_log
ORDER BY processing_timestamp DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 03 — RESUMO DO QUALITY LOG
-- Quantidade total de problemas tratados
SELECT
    source_table,
    COUNT(*) AS total_checks,
    SUM(records_affected) AS total_registros_afetados
FROM {SILVER_SCHEMA}.quality_log
GROUP BY source_table
ORDER BY total_registros_afetados DESC;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 04 — DETECÇÃO DE REPLAY / DUPLICIDADE
-- Evidência do troubleshooting realizado
SELECT
    order_id,
    COUNT(*) AS qtd
FROM {SILVER_SCHEMA}.silver_pedidos_cabecalho
GROUP BY order_id
HAVING COUNT(*) > 1;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 05 — GRANULARIDADE DA FATO
-- Validação de grain
SELECT
    COUNT(*) total,
    COUNT(DISTINCT concat(order_id, '-', item_seq)) granularidade
FROM {GOLD_SCHEMA}.fato_pedidos;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 06 — VOLUME DA FATO

SELECT COUNT(*) AS total_fato
FROM {GOLD_SCHEMA}.fato_pedidos;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 07 — INTEGRIDADE DAS DIMENSÕES
-- Surrogate keys únicas
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
-- 08 — RECEITA POR PERÍODO
-- Evidência da camada executiva
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
-- 09 — MÉTRICAS OPERACIONAIS
-- Cancelamento e atraso
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

display(spark.sql(f"""
-- 10 — SEMANTIC LAYER
-- Evidência da view analítica
SELECT
    order_id,
    nome_cliente,
    nome_produto,
    nome_canal,
    nome_regiao,
    receita_liquida_item,
    status_order,
    status_entrega
FROM {GOLD_SCHEMA}.vw_pedidos_analytics
LIMIT 20;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 11 — INTEGRIDADE DA SEMANTIC LAYER
SELECT
    COUNT(CASE WHEN nome_cliente IS NULL THEN 1 END) AS sem_cliente,
    COUNT(CASE WHEN nome_produto IS NULL THEN 1 END) AS sem_produto,
    COUNT(CASE WHEN nome_canal IS NULL THEN 1 END) AS sem_canal,
    COUNT(CASE WHEN nome_regiao IS NULL THEN 1 END) AS sem_regiao,
    COUNT(CASE WHEN ano IS NULL THEN 1 END) AS sem_data
FROM {GOLD_SCHEMA}.vw_pedidos_analytics;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 12 — DIMENSÃO CALENDÁRIO
-- Cobertura temporal
SELECT
    MIN(data) AS data_min,
    MAX(data) AS data_max
FROM {GOLD_SCHEMA}.dim_data;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 13 — TOP PRODUTOS
-- Exemplo analítico final
SELECT
    nome_produto,
    categoria,
    SUM(receita_liquida_item) AS receita
FROM {GOLD_SCHEMA}.vw_pedidos_analytics
GROUP BY nome_produto, categoria
ORDER BY receita DESC
LIMIT 20;
"""))

# COMMAND ----------

display(spark.sql(f"""
-- 14 - RESUMO EXECUTIVO DO PIPELINE

WITH fato AS (
    SELECT COUNT(*) AS total_fato
    FROM {GOLD_SCHEMA}.fato_pedidos
),

granularidade AS (
    SELECT
        COUNT(DISTINCT concat(order_id, '-', item_seq)) AS granularidade
    FROM {GOLD_SCHEMA}.fato_pedidos
),

clientes AS (
    SELECT COUNT(*) AS total_clientes
    FROM {GOLD_SCHEMA}.dim_cliente
),

produtos AS (
    SELECT COUNT(*) AS total_produtos
    FROM {GOLD_SCHEMA}.dim_produto
),

receita AS (
    SELECT
        ROUND(SUM(receita_liquida), 2) AS receita_total
    FROM {GOLD_SCHEMA}.metricas_por_periodo
),

quality AS (
    SELECT
        COUNT(*) AS total_checks,
        SUM(records_affected) AS registros_tratados
    FROM {SILVER_SCHEMA}.quality_log
),

semantic AS (
    SELECT COUNT(*) AS registros_analytics
    FROM {GOLD_SCHEMA}.vw_pedidos_analytics
)

SELECT
    f.total_fato,
    g.granularidade,
    c.total_clientes,
    p.total_produtos,
    r.receita_total,
    q.total_checks,
    q.registros_tratados,
    s.registros_analytics
FROM fato f
CROSS JOIN granularidade g
CROSS JOIN clientes c
CROSS JOIN produtos p
CROSS JOIN receita r
CROSS JOIN quality q
CROSS JOIN semantic s;
"""))

# COMMAND ----------

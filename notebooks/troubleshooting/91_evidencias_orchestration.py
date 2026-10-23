# Databricks notebook source
# MAGIC %run ../00_setup

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {CONTROL_SCHEMA}.ingestion_log"
))

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {SILVER_SCHEMA}.quality_log"
))

# COMMAND ----------

display(spark.sql(
    f"SELECT * FROM {GOLD_SCHEMA}.vw_pedidos_analytics LIMIT 20"
))

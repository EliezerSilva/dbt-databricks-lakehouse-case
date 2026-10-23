# Databricks notebook source
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F

# COMMAND ----------

pipeline_execution_timestamp = datetime.datetime.utcnow()

# COMMAND ----------

# DBTITLE 1,Produtos
_SOURCE_FILE  = "cadastro_produtos_api_dump.json"
_TARGET_TABLE = "bronze_cadastro_produtos"

if should_process_bronze(_SOURCE_FILE, _TARGET_TABLE):

    df_raw = spark.read.option("multiLine", "true").json(f"{SOURCE_ROOT}/{_SOURCE_FILE}")

    df = add_bronze_cols(
        df_raw.select(
            F.col("product.product_id").alias("product_id"),
            F.col("product.name").alias("product_name"),
            F.col("product.category").alias("category"),
            F.col("product.subcategory").alias("subcategory"),
            F.col("product.status").alias("status"),
            F.col("pricing.list_price").alias("list_price"),
            F.col("pricing.currency").alias("currency"),
            F.col("attributes.family").alias("family"),
            F.col("attributes.tags").cast("string").alias("tags_raw"),
            F.col("updated_at"),
        ),
        _SOURCE_FILE, pipeline_execution_timestamp
    )

    n = write_bronze_table(df, _TARGET_TABLE)
    log_ingestion(
        source_file=_SOURCE_FILE, target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=n, pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
    )

# COMMAND ----------

# DBTITLE 1,Entregas
_SOURCE_FILE  = "logistica_entregas.json"
_TARGET_TABLE = "bronze_logistica_entregas"

if should_process_bronze(_SOURCE_FILE, _TARGET_TABLE):

    df_raw = spark.read.option("multiLine", "true").json(f"{SOURCE_ROOT}/{_SOURCE_FILE}")

    df = add_bronze_cols(
        df_raw.select(
            F.col("delivery_id"),
            F.col("order_ref"),
            F.col("carrier.name").alias("carrier_name"),
            F.col("carrier.mode").alias("carrier_mode"),
            F.col("delivery_status"),
            F.col("timestamps.shipped_at").alias("shipped_at_raw"),
            F.col("timestamps.delivered_at").alias("delivered_at_raw"),
            F.col("destination.state").alias("dest_state"),
            F.col("destination.city").alias("dest_city"),
            F.col("cost"),
        ),
        _SOURCE_FILE, pipeline_execution_timestamp
    )

    n = write_bronze_table(df, _TARGET_TABLE)
    log_ingestion(
        source_file=_SOURCE_FILE, target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=n, pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
    )

# COMMAND ----------

# DBTITLE 1,Ocorrencias
_SOURCE_FILE  = "atendimento_ocorrencias.ndjson"
_TARGET_TABLE = "bronze_atendimento_ocorrencias"

if should_process_bronze(_SOURCE_FILE, _TARGET_TABLE):

    df = add_bronze_cols(
        spark.read.json(f"{SOURCE_ROOT}/{_SOURCE_FILE}"),
        _SOURCE_FILE, pipeline_execution_timestamp
    )

    n = write_bronze_table(df, _TARGET_TABLE)
    log_ingestion(
        source_file=_SOURCE_FILE, target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=n, pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
    )

    elapsed = round((datetime.datetime.utcnow() - pipeline_execution_timestamp).total_seconds(), 1)
    print(f"04_bronze_json_sources concluído em {elapsed}s")
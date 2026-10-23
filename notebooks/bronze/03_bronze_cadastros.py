# Databricks notebook source
# DBTITLE 1,No CE é necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

pipeline_execution_timestamp = datetime.datetime.utcnow()

# COMMAND ----------

# DBTITLE 1,Clientes
_SOURCE_FILE = "crm_clientes_export.xlsx"
_TARGET_TABLE = "bronze_crm_clientes"

if should_process_bronze(_SOURCE_FILE, _TARGET_TABLE):

    pdf = pd.read_excel(f"{SOURCE_ROOT}/{_SOURCE_FILE}", dtype=str)
    df = add_bronze_cols(spark.createDataFrame(pdf), _SOURCE_FILE, pipeline_execution_timestamp)
    n = write_bronze_table(df, _TARGET_TABLE)

    log_ingestion(
        source_file=_SOURCE_FILE,
        target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=n, pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
    )

# COMMAND ----------

# DBTITLE 1,Canais
_SOURCE_FILE = "comercial_canais.xlsx"
_TARGET_TABLE = "bronze_comercial_canais"

if should_process_bronze(_SOURCE_FILE, _TARGET_TABLE):

    pdf = pd.read_excel(f"{SOURCE_ROOT}/{_SOURCE_FILE}", dtype=str)
    df = add_bronze_cols(spark.createDataFrame(pdf), _SOURCE_FILE, pipeline_execution_timestamp)
    n = write_bronze_table(df, _TARGET_TABLE)

    log_ingestion(
        source_file=_SOURCE_FILE,
        target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=n, pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
    )

# COMMAND ----------

# DBTITLE 1,Vendedores
_SOURCE_FILE = "vendedores.csv"
_TARGET_TABLE = "bronze_comercial_vendedores"
_EXPECTED_SEP = ";"
_EXPECTED_COL_COUNT = 6

if should_process_bronze(_SOURCE_FILE, _TARGET_TABLE):

    val_status, val_msg, detected_schema, expected_schema = validate_csv_layout(
        _SOURCE_FILE, _EXPECTED_SEP, _EXPECTED_COL_COUNT
    )

    if val_status != "OK":
        log_ingestion(
            source_file=_SOURCE_FILE,
            target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
            records_loaded=0, pipeline_execution_timestamp=pipeline_execution_timestamp,
            bronze_load_timestamp=datetime.datetime.utcnow(),
            status=val_status, validation_message=val_msg,
            detected_schema=detected_schema, expected_schema=expected_schema,
        )
        raise ValueError(f"Layout inválido em {_SOURCE_FILE}: {val_msg}")

    schema_vend = StructType([
        StructField("seller_id",     StringType(), True),
        StructField("seller_name",   StringType(), True),
        StructField("canal_id",      StringType(), True),
        StructField("regional_code", StringType(), True),
        StructField("hire_date",     StringType(), True),
        StructField("status",        StringType(), True),
    ])

    df_raw = spark.read.option("header", "true").option("sep", ";").schema(schema_vend).csv(f"{SOURCE_ROOT}/{_SOURCE_FILE}")
    df = add_bronze_cols(df_raw, _SOURCE_FILE, pipeline_execution_timestamp)
    n = write_bronze_table(df, _TARGET_TABLE)

    log_ingestion(
        source_file=_SOURCE_FILE,
        target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=n, pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
        status=val_status, validation_message=val_msg,
        detected_schema=detected_schema, expected_schema=expected_schema,
    )

# COMMAND ----------

# DBTITLE 1,Regioes
_SOURCE_FILE = "legado_regioes_pipe.txt"
_TARGET_TABLE = "bronze_legado_regioes"

if should_process_bronze(_SOURCE_FILE, _TARGET_TABLE):

    schema_reg = StructType([
        StructField("regional_code", StringType(), True),
        StructField("regional_name", StringType(), True),
        StructField("state",         StringType(), True),
        StructField("manager_name",  StringType(), True),
        StructField("active_flag",   StringType(), True),
    ])

    df_raw = spark.read.option("header", "true").option("sep", "|").schema(schema_reg).csv(f"{SOURCE_ROOT}/{_SOURCE_FILE}")
    df = add_bronze_cols(df_raw, _SOURCE_FILE, pipeline_execution_timestamp)
    n = write_bronze_table(df, _TARGET_TABLE)

    log_ingestion(
        source_file=_SOURCE_FILE,
        target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=n, pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
    )
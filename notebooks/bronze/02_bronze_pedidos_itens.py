# Databricks notebook source
# DBTITLE 1,No CE é necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# COMMAND ----------

_SOURCE_FILE        = "erp_pedidos_itens_2025.csv"
_TARGET_TABLE       = "bronze_erp_pedidos_itens"
_EXPECTED_SEP       = ","
_EXPECTED_COL_COUNT = 7

pipeline_execution_timestamp = datetime.datetime.utcnow()

# COMMAND ----------

# Proteção de replay e recuperação da carga Bronze DEV.
_target = f"{BRONZE_SCHEMA}.{_TARGET_TABLE}"
_source_mtime = get_file_last_modified(_SOURCE_FILE)

if _source_mtime is None:
    raise RuntimeError(f"Arquivo não encontrado ou sem metadados: {_SOURCE_FILE}")

_already_loaded = check_already_loaded(_SOURCE_FILE, _target)
_table_exists = spark.catalog.tableExists(_target)

if _already_loaded:
    if not _table_exists:
        raise RuntimeError(
            f"Log OK encontrado, mas tabela ausente: {_target}"
        )

    _history = (
        spark.table(f"{CONTROL_SCHEMA}.ingestion_log")
        .filter(
            (F.col("source_file") == _SOURCE_FILE)
            & (F.col("target_table") == _target)
            & (F.col("status") == "OK")
        )
        .select("source_file_last_modified")
        .collect()
    )

    if not _history or any(
        row["source_file_last_modified"] is None
        or row["source_file_last_modified"] != _source_mtime
        for row in _history
    ):
        raise RuntimeError(
            "Arquivo alterado ou histórico inconsistente. "
            "Reprocessamento requer avaliação explícita."
        )

    print(f"[SKIP] Carga já concluída: {_target}")
    dbutils.notebook.exit("SKIPPED_ALREADY_LOADED")

if _table_exists:
    raise RuntimeError(
        f"Tabela {_target} existe sem log OK. "
        "Possível falha anterior: investigar antes de sobrescrever."
    )



# COMMAND ----------

val_status, val_msg, detected_schema, expected_schema = validate_csv_layout(
    _SOURCE_FILE, _EXPECTED_SEP, _EXPECTED_COL_COUNT
)

if val_status != "OK":
    log_ingestion(
        source_file=_SOURCE_FILE,
        target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
        records_loaded=0,
        pipeline_execution_timestamp=pipeline_execution_timestamp,
        bronze_load_timestamp=datetime.datetime.utcnow(),
        status=val_status,
        validation_message=val_msg,
        detected_schema=detected_schema,
        expected_schema=expected_schema,
    )
    raise ValueError(f"Layout inválido em {_SOURCE_FILE}: {val_msg}")

# COMMAND ----------

schema = StructType([
    StructField("order_id",     StringType(),  True),
    StructField("item_seq",     IntegerType(), True),
    StructField("product_code", StringType(),  True),
    StructField("quantity",     DoubleType(),  True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("total_item",   DoubleType(),  True),
    StructField("item_status",  StringType(),  True),
])

df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", _EXPECTED_SEP)
    .schema(schema)
    .csv(f"{SOURCE_ROOT}/{_SOURCE_FILE}")
)

df_bronze = add_bronze_cols(df_raw, _SOURCE_FILE, pipeline_execution_timestamp)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ingestion_date")
    .saveAsTable(f"{BRONZE_SCHEMA}.{_TARGET_TABLE}")
)

bronze_load_timestamp = datetime.datetime.utcnow()
elapsed = round((bronze_load_timestamp - pipeline_execution_timestamp).total_seconds(), 1)

# COMMAND ----------

records_loaded = df_bronze.count()

log_ingestion(
    source_file=_SOURCE_FILE,
    target_table=f"{BRONZE_SCHEMA}.{_TARGET_TABLE}",
    records_loaded=records_loaded,
    pipeline_execution_timestamp=pipeline_execution_timestamp,
    bronze_load_timestamp=bronze_load_timestamp,
    status=val_status,
    validation_message=val_msg,
    detected_schema=detected_schema,
    expected_schema=expected_schema,
)

print(f"{_TARGET_TABLE}: {records_loaded} rows | {elapsed}s | status={val_status}")
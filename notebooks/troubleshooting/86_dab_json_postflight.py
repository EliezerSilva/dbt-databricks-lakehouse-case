# Databricks notebook source
# MAGIC %run ../00_runtime_config

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

assert CATALOG == "workspace"
assert BRONZE_SCHEMA == "bronze_dev"
assert CONTROL_SCHEMA == "control_dev"

sources = [
    ("cadastro_produtos_api_dump.json", "bronze_cadastro_produtos", 72),
    ("logistica_entregas.json", "bronze_logistica_entregas", 322),
    ("atendimento_ocorrencias.ndjson", "bronze_atendimento_ocorrencias", 270),
]

log_table = f"{CATALOG}.{CONTROL_SCHEMA}.ingestion_log"
assert spark.catalog.tableExists(log_table)

total_rows = 0

for filename, table_name, expected_rows in sources:
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    source_path = f"{SOURCE_ROOT}/{filename}"

    files = dbutils.fs.ls(source_path)

    assert len(files) == 1
    assert files[0].name.rstrip("/") == filename
    assert not files[0].isDir()

    assert spark.catalog.tableExists(target), f"Tabela ausente: {target}"

    actual_rows = spark.table(target).count()

    history = (
        spark.table(log_table)
        .filter(
            (F.col("source_file") == filename)
            & (F.col("target_table") == f"{BRONZE_SCHEMA}.{table_name}")
        )
        .collect()
    )

    assert len(history) == 1, (
        f"{table_name}: esperado 1 log, encontrados {len(history)}"
    )

    row = history[0]

    assert row["status"] == "OK"
    assert actual_rows == expected_rows, (
        f"{table_name}: esperado {expected_rows}, encontrado {actual_rows}"
    )
    assert row["records_loaded"] == actual_rows

    source_mtime = datetime.utcfromtimestamp(
        files[0].modificationTime / 1000
    )

    assert row["source_file_last_modified"] == source_mtime, (
        f"Timestamp divergente: {filename}"
    )

    total_rows += actual_rows

    print(
        f"VALIDATED {table_name}: "
        f"rows={actual_rows}, "
        f"log={row['records_loaded']}, "
        f"status={row['status']}"
    )

assert total_rows == 664
print(f"TOTAL_VALIDATED: {total_rows}")
print("DAB_JSON_POSTFLIGHT_SUCCESS")

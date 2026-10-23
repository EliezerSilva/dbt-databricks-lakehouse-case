# Databricks notebook source
# MAGIC %run ../00_runtime_config

# COMMAND ----------

from pyspark.sql import functions as F

assert CATALOG == "workspace"
assert BRONZE_SCHEMA == "bronze_dev"
assert CONTROL_SCHEMA == "control_dev"

sources = [
    ("crm_clientes_export.xlsx", "bronze_crm_clientes"),
    ("comercial_canais.xlsx", "bronze_comercial_canais"),
    ("vendedores.csv", "bronze_comercial_vendedores"),
    ("legado_regioes_pipe.txt", "bronze_legado_regioes"),
]

log_table = f"{CATALOG}.{CONTROL_SCHEMA}.ingestion_log"
assert spark.catalog.tableExists(log_table)

for filename, table_name in sources:
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    source_path = f"{SOURCE_ROOT}/{filename}"

    files = dbutils.fs.ls(source_path)

    assert len(files) == 1
    assert files[0].name.rstrip("/") == filename
    assert not files[0].isDir()

    assert spark.catalog.tableExists(target), (
        f"Tabela ausente: {target}"
    )

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
    assert row["records_loaded"] == actual_rows
    assert actual_rows > 0

    from datetime import datetime
    source_mtime = datetime.utcfromtimestamp(
        files[0].modificationTime / 1000
    )

    assert row["source_file_last_modified"] == source_mtime, (
        f"Timestamp divergente: {filename}"
    )

    print(
        f"VALIDATED {table_name}: "
        f"rows={actual_rows}, "
        f"log={row['records_loaded']}, "
        f"status={row['status']}"
    )

print("DAB_CADASTROS_POSTFLIGHT_SUCCESS")

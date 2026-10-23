# Databricks notebook source
# MAGIC %run ../00_runtime_config

# COMMAND ----------

from pyspark.sql import functions as F

source_file = "erp_pedidos_cabecalho_2025.csv"
table_name = "bronze_erp_pedidos_cabecalho"
target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
log_table = f"{CATALOG}.{CONTROL_SCHEMA}.ingestion_log"

assert CATALOG == "workspace"
assert BRONZE_SCHEMA == "bronze_dev"
assert CONTROL_SCHEMA == "control_dev"

print("=== SOURCE FILE ===")
source_path = f"{SOURCE_ROOT}/{source_file}"
files = dbutils.fs.ls(source_path)

print("Requested source:", source_path)
print("Objects returned:", len(files))

for item in files:
    print(
        "FOUND:",
        item.path,
        "name=", item.name,
        "size=", item.size,
        "mtime=", item.modificationTime
    )

assert len(files) == 1, (
    f"Esperado um arquivo, encontrados {len(files)}"
)

source = files[0]

assert source.name.rstrip("/") == source_file, (
    f"Arquivo diferente do esperado: {source.name!r}"
)

assert not source.isDir(), (
    f"Origem é um diretório: {source.path}"
)

assert source.size > 0, (
    f"Arquivo vazio: {source.path}"
)

print("Source:", source.path)
print("Size:", source.size)
print("Modified:", source.modificationTime)

print("=== BRONZE TARGET ===")
target_exists = spark.catalog.tableExists(target)
print("Target:", target)
print("Exists:", target_exists)

if target_exists:
    print(
        "Rows:",
        spark.table(target).count()
    )

print("=== INGESTION LOG ===")
assert spark.catalog.tableExists(log_table), (
    f"Tabela de controle ausente: {log_table}"
)

history = (
    spark.table(log_table)
    .filter(
        (F.col("source_file") == source_file)
        & (F.col("target_table").isin(
            f"{BRONZE_SCHEMA}.{table_name}",
            target
        ))
    )
)

history.select(
    "source_file",
    "target_table",
    "status",
    "records_loaded",
    "source_file_last_modified",
    "bronze_load_timestamp"
).show(truncate=False)

print("History rows:", history.count())
print("DAB_BRONZE_PREFLIGHT_SUCCESS")

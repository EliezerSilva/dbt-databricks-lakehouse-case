# Databricks notebook source
# MAGIC %run ../00_runtime_config

# COMMAND ----------

from pyspark.sql import functions as F

assert CATALOG == "workspace"
assert BRONZE_SCHEMA == "bronze_dev"
assert CONTROL_SCHEMA == "control_dev"

sources = [
    ("cadastro_produtos_api_dump.json", "bronze_cadastro_produtos", True),
    ("logistica_entregas.json", "bronze_logistica_entregas", True),
    ("atendimento_ocorrencias.ndjson", "bronze_atendimento_ocorrencias", False),
]

log_table = f"{CATALOG}.{CONTROL_SCHEMA}.ingestion_log"
assert spark.catalog.tableExists(log_table)

for filename, table_name, multiline in sources:
    print(f"=== SOURCE: {filename} ===")

    path = f"{SOURCE_ROOT}/{filename}"
    files = dbutils.fs.ls(path)

    assert len(files) == 1
    assert files[0].name.rstrip("/") == filename
    assert not files[0].isDir()
    assert files[0].size > 0

    print("Size:", files[0].size)
    print("Modified:", files[0].modificationTime)

    df = spark.read.option(
        "multiLine", str(multiline).lower()
    ).json(path)

    rows = df.count()
    assert rows > 0, f"JSON vazio: {filename}"

    print("Rows:", rows)
    print("Columns:", df.columns)

    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    exists = spark.catalog.tableExists(target)

    history = (
        spark.table(log_table)
        .filter(
            (F.col("source_file") == filename)
            & F.col("target_table").isin(
                f"{BRONZE_SCHEMA}.{table_name}",
                target
            )
        )
    )

    history_rows = history.count()

    print("Target:", target)
    print("Exists:", exists)
    print("History rows:", history_rows)

    assert not exists, f"Tabela já existe: {target}"
    assert history_rows == 0, (
        f"Histórico preexistente: {filename}"
    )


# COMMAND ----------

print("=== VALIDANDO SCHEMAS ANINHADOS ===")

produtos = (
    spark.read.option("multiLine", "true")
    .json(f"{SOURCE_ROOT}/cadastro_produtos_api_dump.json")
)

produtos_validado = produtos.select(
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
)

assert produtos_validado.count() == 72
print("SCHEMA_OK produtos: 72 registros")

entregas = (
    spark.read.option("multiLine", "true")
    .json(f"{SOURCE_ROOT}/logistica_entregas.json")
)

entregas_validado = entregas.select(
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
)

assert entregas_validado.count() == 322
print("SCHEMA_OK entregas: 322 registros")

ocorrencias = spark.read.json(
    f"{SOURCE_ROOT}/atendimento_ocorrencias.ndjson"
)

expected_columns = {
    "created_at",
    "customer_code",
    "event_type",
    "metadata",
    "order_id",
    "severity",
    "status",
    "ticket_id",
}

assert expected_columns.issubset(set(ocorrencias.columns))
assert ocorrencias.count() == 270

print("SCHEMA_OK ocorrencias: 270 registros")
print("DAB_JSON_SCHEMA_VALIDATED")

print("DAB_JSON_PREFLIGHT_SUCCESS")

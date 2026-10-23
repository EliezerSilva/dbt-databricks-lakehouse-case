# Databricks notebook source
# MAGIC %run ../00_runtime_config

# COMMAND ----------

from pyspark.sql import functions as F

assert CATALOG == "workspace"
assert BRONZE_SCHEMA == "bronze_dev"
assert SILVER_SCHEMA == "silver_dev"
assert CONTROL_SCHEMA == "control_dev"

bronze = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_erp_pedidos_cabecalho"
silver = f"{CATALOG}.{SILVER_SCHEMA}.silver_pedidos_cabecalho"

quality = f"{CATALOG}.{SILVER_SCHEMA}.quality_log"
invalid = f"{CATALOG}.{SILVER_SCHEMA}.tabelas_invalidas"

print("=== DESTINOS SILVER ===")

assert not spark.catalog.tableExists(silver), (
    f"ABORTADO: destino já existe: {silver}"
)

for table in [quality, invalid]:
    assert spark.catalog.tableExists(table), (
        f"Tabela de auditoria ausente: {table}"
    )

    records = (
        spark.table(table)
        .filter(F.col("source_table") == "silver_pedidos_cabecalho")
        .count()
    )

    print(f"{table}: registros existentes={records}")

    assert records == 0, (
        f"Histórico Silver 01 preexistente: {table}"
    )

assert spark.catalog.tableExists(bronze)
df = spark.table(bronze)

expected = {
    "order_id",
    "status_order",
    "order_date",
    "promised_date",
    "last_update",
    "gross_amount",
    "net_amount",
    "discount_amount",
    "payment_details",
}

missing = expected - set(df.columns)

assert not missing, f"Colunas ausentes: {sorted(missing)}"

print("\n=== QUALIDADE BRONZE ===")

total = df.count()
null_ids = df.filter(F.col("order_id").isNull()).count()

duplicates = (
    df.filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_groups = duplicates.count()

tie_groups = (
    df.filter(F.col("order_id").isNotNull())
    .groupBy("order_id", "last_update")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_status = df.filter(F.col("status_order").isNull()).count()
null_gross = df.filter(F.col("gross_amount").isNull()).count()

print(f"Total Bronze: {total}")
print(f"Order IDs nulos: {null_ids}")
print(f"Grupos com order_id duplicado: {duplicate_groups}")
print(f"Empates order_id + last_update: {tie_groups}")
print(f"Status nulos: {null_status}")
print(f"Gross amount nulos: {null_gross}")

assert total == 403, f"Contagem Bronze inesperada: {total}"


# COMMAND ----------

print("=== ANALISE DE DUPLICIDADES ===")

from pyspark.sql import Window

base = df.filter(F.col("order_id").isNotNull())

duplicate_ids = (
    base.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .select("order_id")
)

duplicated_rows = base.join(
    duplicate_ids, "order_id", "inner"
)

print("Registros nos grupos duplicados:", duplicated_rows.count())

print("=== GRUPOS DUPLICADOS ===")
(
    duplicated_rows.groupBy("order_id")
    .agg(
        F.count("*").alias("total"),
        F.countDistinct("last_update").alias("timestamps_distintos"),
        F.countDistinct(
            F.to_json(F.struct(*[
                F.col(c) for c in duplicated_rows.columns
            ]))
        ).alias("conteudos_distintos")
    )
    .orderBy("order_id")
    .show(truncate=False)
)

print("=== EMPATES DE ULTIMA ATUALIZACAO ===")

tie_keys = (
    base.groupBy("order_id", "last_update")
    .count()
    .filter(F.col("count") > 1)
    .select("order_id", "last_update")
)

tie_rows = base.join(
    tie_keys,
    ["order_id", "last_update"],
    "inner"
)

(
    tie_rows.groupBy("order_id", "last_update")
    .agg(
        F.count("*").alias("total"),
        F.countDistinct(
            F.to_json(F.struct(*[
                F.col(c) for c in tie_rows.columns
            ]))
        ).alias("conteudos_distintos")
    )
    .orderBy("order_id", "last_update")
    .show(truncate=False)
)


print("=== DETALHES DO EMPATE 000121 ===")

from pyspark.sql import functions as F

rows_121 = (
    df.filter(
        F.upper(F.trim(F.col("order_id").cast("string"))) == "O00121"
    )
    .orderBy("last_update")
)

actual_count = rows_121.count()
print(f"PEDIDO_121_REGISTROS: {actual_count}")

assert actual_count == 2, (
    f"Esperados 2 registros para pedido 121; encontrados {actual_count}"
)

for column in sorted(rows_121.columns):
    values = (
        rows_121.select(
            F.col(column).cast("string").alias("value")
        )
        .distinct()
        .collect()
    )

    if len(values) > 1:
        print(
            f"CAMPO_DIFERENTE {column}: "
            f"{[row['value'] for row in values]}"
        )

print("SILVER_CABECALHO_TIE_DETAILS")

print("SILVER_CABECALHO_DUPLICATES_ANALYZED")

print("SILVER_CABECALHO_PREFLIGHT_SUCCESS")

# COMMAND ----------

import json

diagnostic_rows = (
    df.filter(
        F.upper(F.trim(F.col("order_id").cast("string"))) == "O00121"
    )
    .collect()
)

assert len(diagnostic_rows) == 2, (
    f"Esperados 2 registros, encontrados {len(diagnostic_rows)}"
)

technical_columns = {
    "source_file",
    "processing_timestamp",
    "ingestion_date",
}

differences = {}

for column in sorted(df.columns):
    if column in technical_columns:
        continue

    values = [
        str(row[column]) if row[column] is not None else None
        for row in diagnostic_rows
    ]

    if values[0] != values[1]:
        differences[column] = values

result = {
    "diagnostic": "SILVER01_DIAGNOSTIC_JSON",
    "order_id": "O00121",
    "records": len(diagnostic_rows),
    "different_business_fields": differences,
}

print(json.dumps(result, ensure_ascii=False, indent=2))

dbutils.notebook.exit(
    json.dumps(result, ensure_ascii=False, default=str)
)

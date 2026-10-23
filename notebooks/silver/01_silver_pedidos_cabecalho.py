# Databricks notebook source
# DBTITLE 1,No CE pode ser necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

SILVER_TABLE = "silver_pedidos_cabecalho"
t0 = datetime.datetime.utcnow()


# Protecao fail-closed antes de qualquer escrita Silver.
_recover_existing_quality = False
_silver_target = f"{SILVER_SCHEMA}.{SILVER_TABLE}"

if spark.catalog.tableExists(_silver_target):
    raise RuntimeError(
        f"ABORTADO: tabela Silver ja existe: {_silver_target}. "
        "Investigar antes de reprocessar."
    )

for _audit_table in ("quality_log", "tabelas_invalidas"):
    _audit_target = f"{SILVER_SCHEMA}.{_audit_table}"

    if not spark.catalog.tableExists(_audit_target):
        raise RuntimeError(
            f"ABORTADO: tabela de auditoria ausente: {_audit_target}"
        )

    _existing = (
        spark.table(_audit_target)
        .filter(F.col("source_table") == SILVER_TABLE)
        .limit(1)
        .count()
    )

    if _existing:
        if _audit_table == "quality_log":
            _previous_rows = (
                spark.table(_audit_target)
                .filter(F.col("source_table") == SILVER_TABLE)
                .select("check_name", "records_affected")
                .collect()
            )

            if len(_previous_rows) != 2:
                raise RuntimeError(
                    "ABORTADO: recuperacao exige exatamente "
                    "2 registros de qualidade; encontrados "
                    f"{len(_previous_rows)}."
                )

            _previous_checks = {
                row["check_name"]: int(row["records_affected"])
                for row in _previous_rows
            }

            _expected_checks = {
                "status_order_desconhecido": 64,
                "gross_amount_derivado": 15,
            }

            if _previous_checks != _expected_checks:
                raise RuntimeError(
                    "ABORTADO: auditoria anterior diferente da "
                    "recuperacao conhecida."
                )

            _recover_existing_quality = True
        else:
            raise RuntimeError(
                f"ABORTADO: auditoria anterior encontrada em {_audit_target}."
            )

print("SILVER01_FIRST_LOAD_GUARD_OK")

df_raw = spark.read.table(f"{BRONZE_SCHEMA}.bronze_erp_pedidos_cabecalho")

# COMMAND ----------

# DBTITLE 1,quarantine - order_id nulo
df_invalidos = df_raw.filter(F.col("order_id").isNull())
if df_invalidos.count() > 0:
    write_invalidos(df_invalidos, SILVER_TABLE, "order_id nulo")
    log_quality(SILVER_TABLE, "order_id_nulo", df_invalidos.count(), "registros sem order_id descartados")

df = df_raw.filter(F.col("order_id").isNotNull())

# COMMAND ----------

# DBTITLE 1,cleansing
def parse_date_col(col_name):
    return F.coalesce(
        F.try_to_timestamp(F.col(col_name), F.lit("yyyy-MM-dd")).cast("date"),
        F.try_to_timestamp(F.col(col_name), F.lit("yyyy/MM/dd")).cast("date"),
        F.try_to_timestamp(F.col(col_name), F.lit("dd/MM/yyyy")).cast("date"),
    )


w_order = Window.partitionBy("order_id").orderBy(
    F.to_timestamp(
        F.col("last_update"), "yyyy-MM-dd HH:mm:ss"
    ).desc_nulls_last(),
    F.when(
        parse_date_col("order_date").isNotNull(), 1
    ).otherwise(0).desc(),
    F.sha2(
        F.to_json(F.struct(
            F.col("order_id"),
            F.col("customer_code"),
            F.col("seller_id"),
            F.col("order_date"),
            F.col("promised_date"),
            F.col("status_order"),
            F.col("gross_amount"),
            F.col("discount_amount"),
            F.col("net_amount"),
            F.col("last_update"),
            F.col("order_source"),
            F.col("order_priority"),
        )), 256
    ).asc()
)

df_silver = (
    df
    .withColumn(
        "status_order",
        F.when(F.col("status_order").isNull(), "DESCONHECIDO")
        .otherwise(
            F.regexp_replace(
                F.upper(F.trim(F.col("status_order"))),
                " ",
                "_"
            )
        )
    )
    .withColumn("order_date", parse_date_col("order_date"))
    .withColumn("promised_date", parse_date_col("promised_date"))
    .withColumn("last_update", F.to_timestamp(F.col("last_update"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("gross_amount",
        F.when(F.col("gross_amount").isNull(), F.col("net_amount") + F.col("discount_amount"))
         .otherwise(F.col("gross_amount"))
    )
    .withColumn("order_source", F.get_json_object(F.col("payment_details"), "$.source"))
    .withColumn("order_priority", F.get_json_object(F.col("payment_details"), "$.priority"))
    .drop("payment_details", "source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
    .withColumn("rn", F.row_number().over(w_order))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

# DBTITLE 1,quality log
n_sem_status = df_silver.filter(F.col("status_order") == "DESCONHECIDO").count()
n_sem_data = df_silver.filter(F.col("order_date").isNull()).count()
n_gross_nulo = df_raw.filter(F.col("gross_amount").isNull()).count()

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("append")
        .partitionBy("status_order")
    .saveAsTable(f"{SILVER_SCHEMA}.{SILVER_TABLE}")
)


# Auditoria posterior a persistencia da tabela Silver.
if _recover_existing_quality:
    print("SILVER01_RECOVERY_QUALITY_LOGS_PRESERVED")
else:
    log_quality(SILVER_TABLE, "status_order_desconhecido", n_sem_status, f"{n_sem_status} pedidos com status_order nulo")
    log_quality(SILVER_TABLE, "order_date_invalida", n_sem_data,   f"{n_sem_data} pedidos sem order_date após parse em 3 formatos")
    log_quality(SILVER_TABLE, "gross_amount_derivado", n_gross_nulo, f"{n_gross_nulo} registros com gross_amount recalculado")

elapsed = round((datetime.datetime.utcnow() - t0).total_seconds(), 1)
print(f"silver.{SILVER_TABLE}: {df_silver.count()} rows | {elapsed}s")
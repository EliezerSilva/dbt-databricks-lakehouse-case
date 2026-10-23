# Databricks notebook source
# DBTITLE 1,No CE pode ser necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

SILVER_TABLE = "silver_pedidos_itens"
t0 = datetime.datetime.utcnow()


# Protecao fail-closed antes de qualquer escrita.
_silver_target = f"{SILVER_SCHEMA}.{SILVER_TABLE}"

if spark.catalog.tableExists(_silver_target):
    raise RuntimeError(f"ABORTADO: tabela ja existe: {_silver_target}")

for _audit_name in ("quality_log", "tabelas_invalidas"):
    _audit_target = f"{SILVER_SCHEMA}.{_audit_name}"

    if not spark.catalog.tableExists(_audit_target):
        raise RuntimeError(f"ABORTADO: auditoria ausente: {_audit_target}")

    if (
        spark.table(_audit_target)
        .filter(F.col("source_table") == SILVER_TABLE)
        .limit(1)
        .count() > 0
    ):
        raise RuntimeError(f"ABORTADO: auditoria anterior: {_audit_target}")

print("SILVER02_FIRST_LOAD_GUARD_OK")

df_raw = spark.read.table(f"{BRONZE_SCHEMA}.bronze_erp_pedidos_itens")

# COMMAND ----------


# Referencia de produtos sem multiplicacao de registros.
df_produtos = (
    spark.read.table(f"{BRONZE_SCHEMA}.bronze_cadastro_produtos")
    .select(
        F.upper(F.trim(F.col("product_id"))).alias("_product_key"),
        F.lower(F.trim(F.col("status"))).alias("_product_status")
    )
    .groupBy("_product_key")
    .agg(
        F.max(
            F.when(F.col("_product_status") == "ativo", 1)
             .otherwise(0)
        ).alias("_produto_ativo")
    )
)

df_classificado = (
    df_raw
    .withColumn("_order_key", F.upper(F.trim(F.col("order_id"))))
    .withColumn("_product_key", F.upper(F.trim(F.col("product_code"))))
    .join(df_produtos, on="_product_key", how="left")
    .withColumn(
        "_produto_cadastrado",
        F.when(F.col("_produto_ativo").isNotNull(), 1).otherwise(0)
    )
    .withColumn(
        "_produto_ativo",
        F.coalesce(F.col("_produto_ativo"), F.lit(0))
    )
    .withColumn(
        "_quantity_presente",
        F.when(F.col("quantity").isNotNull(), 1).otherwise(0)
    )
    .withColumn(
        "_business_hash",
        F.sha2(
            F.to_json(F.struct(
                F.col("order_id"),
                F.col("item_seq"),
                F.col("product_code"),
                F.col("quantity"),
                F.col("unit_price"),
                F.col("total_item"),
                F.col("item_status")
            )),
            256
        )
    )
)

dedup_window = Window.partitionBy(
    "_order_key", "item_seq"
).orderBy(
    F.col("_produto_ativo").desc(),
    F.col("_produto_cadastrado").desc(),
    F.col("_quantity_presente").desc(),
    F.col("processing_timestamp").desc_nulls_last(),
    F.col("_business_hash").asc()
)

df_dedup = (
    df_classificado
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop(
        "rn",
        "_order_key",
        "_product_key",
        "_produto_ativo",
        "_produto_cadastrado",
        "_quantity_presente",
        "_business_hash"
    )
)

n_dupl = df_raw.count() - df_dedup.count()
# Auditoria de duplicidades apos persistencia.

# COMMAND ----------

df_invalidos = df_dedup.filter(F.col("quantity").isNull())
n_invalidos = df_invalidos.count()

# Quarentena e auditoria apos persistencia.

df = df_dedup.filter(F.col("quantity").isNotNull())

# COMMAND ----------

df_silver = (
    df
    .withColumn(
        "item_status",
        F.when(F.col("item_status").isNull(), "NAO_INFORMADO")
        .otherwise(
            F.regexp_replace(
                F.upper(F.trim(F.col("item_status"))),
                " ",
                "_"
            )
        )
    )
    .withColumn("total_item",
        F.when(
            F.abs(F.col("total_item") - (F.col("quantity") * F.col("unit_price"))) > 0.01,
            F.round(F.col("quantity") * F.col("unit_price"), 2)
        ).otherwise(F.col("total_item"))
    )
    .drop("source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

n_status_nulo = df_silver.filter(F.col("item_status") == "NAO_INFORMADO").count()
n_total_recomp = df.filter(
    F.abs(F.col("total_item") - (F.col("quantity") * F.col("unit_price"))) > 0.01
).count()

# Auditoria de status apos persistencia.
# Auditoria financeira apos persistencia.

# COMMAND ----------


# Validacoes antes da primeira escrita.
_n_silver = df_silver.count()

_n_chaves = (
    df_silver
    .select(
        F.upper(F.trim(F.col("order_id"))).alias("_pedido"),
        F.col("item_seq")
    )
    .distinct()
    .count()
)

if _n_silver <= 0 or _n_silver != _n_chaves:
    raise RuntimeError(
        f"ABORTADO: Silver 02 invalida: "
        f"linhas={_n_silver}, chaves={_n_chaves}"
    )

if _n_silver + n_invalidos + n_dupl != df_raw.count():
    raise RuntimeError("ABORTADO: reconciliacao Bronze/Silver divergente")

print(f"SILVER02_VALIDATED_ROWS={_n_silver}")

(
    df_silver.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{SILVER_SCHEMA}.{SILVER_TABLE}")
)


# Persistencia de auditoria somente apos a tabela Silver.
log_quality(
    SILVER_TABLE, "duplicatas_removidas",
    n_dupl, f"{n_dupl} duplicatas removidas por (order_id, item_seq)"
)

if n_invalidos > 0:
    write_invalidos(df_invalidos, SILVER_TABLE, "quantity nula")
    log_quality(
        SILVER_TABLE, "quantity_nula",
        n_invalidos, "itens sem quantity descartados"
    )

log_quality(
    SILVER_TABLE, "item_status_nao_informado",
    n_status_nulo, f"{n_status_nulo} itens com item_status nulo"
)

log_quality(
    SILVER_TABLE, "total_item_recomputado",
    n_total_recomp,
    f"{n_total_recomp} itens com total_item divergente — recomputados"
)

elapsed = round((datetime.datetime.utcnow() - t0).total_seconds(), 1)
print(f"silver.{SILVER_TABLE}: {df_silver.count()} rows | {elapsed}s")
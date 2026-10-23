# Databricks notebook source
# DBTITLE 1,No CE pode ser necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F

# COMMAND ----------

t0 = datetime.datetime.utcnow()

def write_silver(df, table_name):
    (
        df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_SCHEMA}.{table_name}")
    )
    n = df.count()
    print(f"silver.{table_name}: {n} rows")
    return n

# COMMAND ----------

df_prod_raw = spark.read.table(f"{BRONZE_SCHEMA}.bronze_cadastro_produtos")
n_status_nulo = df_prod_raw.filter(F.col("status").isNull()).count()

df_produtos = (
    df_prod_raw
    .withColumn("status",
        F.when(F.col("status").isNull(), "NAO_INFORMADO")
         .otherwise(F.initcap(F.lower(F.trim(F.col("status")))))
    )
    .withColumn("category", F.initcap(F.lower(F.trim(F.col("category")))))
    .withColumn("subcategory", F.initcap(F.lower(F.trim(F.col("subcategory")))))
    .withColumn("family", F.initcap(F.lower(F.trim(F.col("family")))))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    .withColumn("tags_csv",
        F.regexp_replace(F.regexp_replace(F.col("tags_raw"), r'[\[\]"]', ""), r",\s*", ",")
    )
    .drop("tags_raw", "source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

log_quality("silver_produtos", "status_nao_informado", n_status_nulo, f"{n_status_nulo} produtos com status nulo")
write_silver(df_produtos, "silver_produtos")

# COMMAND ----------

df_ent_raw = spark.read.table(f"{BRONZE_SCHEMA}.bronze_logistica_entregas")

delivery_status_map = {
    "delivered": "entregue",
    "Delivered": "entregue",
    "in_transit": "em_transito",
    "cancelled": "cancelado",
    "atrasado": "atrasado",
}

status_expr = F.col("delivery_status")
for raw_val, canonical in delivery_status_map.items():
    status_expr = F.when(F.col("delivery_status") == raw_val, F.lit(canonical)).otherwise(status_expr)
status_expr = F.when(F.col("delivery_status").isNull(), F.lit("NAO_INFORMADO")).otherwise(status_expr)

df_entregas = (
    df_ent_raw
    .withColumn("delivery_status", status_expr)
    .withColumn("carrier_name",
        F.when(F.col("carrier_name").isNull(), F.lit("NAO_INFORMADO")).otherwise(F.col("carrier_name"))
    )
    .withColumn(
    "shipped_at",
    F.coalesce(
        F.try_to_timestamp(F.col("shipped_at_raw"), F.lit("dd/MM/yyyy HH:mm")),
        F.try_to_timestamp(F.col("shipped_at_raw"), F.lit("yyyy-MM-dd'T'HH:mm:ss"))
    )
    )
    .withColumn(
        "delivered_at",
        F.coalesce(
            F.try_to_timestamp(F.col("delivered_at_raw"), F.lit("dd/MM/yyyy HH:mm")),
            F.try_to_timestamp(F.col("delivered_at_raw"), F.lit("yyyy-MM-dd'T'HH:mm:ss"))
        )
    )
    .drop("shipped_at_raw", "delivered_at_raw", "source_file", "processing_timestamp", "ingestion_date")
)

df_pedidos_cab = spark.read.table(f"{SILVER_SCHEMA}.silver_pedidos_cabecalho").select("order_id", "promised_date").distinct()

df_logistica = (
    df_entregas
    .join(df_pedidos_cab, df_entregas["order_ref"] == df_pedidos_cab["order_id"], how="left")
    .withColumn("flag_atrasado",
        F.when(
            (F.col("delivery_status") == "atrasado") |
            (F.col("delivered_at").cast("date") > F.col("promised_date")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn("dias_atraso",
        F.when(
            F.col("delivered_at").isNotNull() & F.col("promised_date").isNotNull(),
            F.datediff(F.col("delivered_at").cast("date"), F.col("promised_date"))
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn("tempo_entrega_dias",
        F.when(
            F.col("shipped_at").isNotNull() & F.col("delivered_at").isNotNull(),
            F.datediff(F.col("delivered_at").cast("date"), F.col("shipped_at").cast("date"))
        ).otherwise(F.lit(None).cast("int"))
    )
    .drop("order_id")
    .withColumn("processing_timestamp", F.current_timestamp())
)

n_carrier_nulo = df_ent_raw.filter(F.col("carrier_name").isNull()).count()
log_quality("silver_logistica", "carrier_name_nao_informado", n_carrier_nulo, f"{n_carrier_nulo} entregas sem carrier_name")
write_silver(df_logistica, "silver_logistica")

# COMMAND ----------

df_ocorr_raw = spark.read.table(f"{BRONZE_SCHEMA}.bronze_atendimento_ocorrencias")

n_event_nulo = df_ocorr_raw.filter(F.col("event_type").isNull()).count()
n_severity_nulo = df_ocorr_raw.filter(F.col("severity").isNull()).count()

df_ocorrencias = (
    df_ocorr_raw
    .withColumn("event_type",
        F.when(F.col("event_type").isNull(), "NAO_INFORMADO").otherwise(F.lower(F.trim(F.col("event_type"))))
    )
    .withColumn("severity",
        F.when(F.col("severity").isNull(), "NAO_INFORMADO").otherwise(F.lower(F.trim(F.col("severity"))))
    )
    .withColumn("status",
        F.when(F.col("status").isNull(), "NAO_INFORMADO").otherwise(F.lower(F.trim(F.col("status"))))
    )
    .withColumn(
        "created_at",
        F.coalesce(
            F.try_to_timestamp(
                F.col("created_at"),
                F.lit("yyyy-MM-dd HH:mm:ss")
            ),
            F.try_to_timestamp(
                F.col("created_at"),
                F.lit("yyyy-MM-dd'T'HH:mm:ss")
            ),
            F.try_to_timestamp(
                F.col("created_at"),
                F.lit("yyyy/MM/dd")
            ),
        )
    )
    .drop("source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

log_quality("silver_atendimento_ocorrencias", "event_type_nao_informado", n_event_nulo,    f"{n_event_nulo} ocorrências com event_type nulo")
log_quality("silver_atendimento_ocorrencias", "severity_nao_informado",   n_severity_nulo, f"{n_severity_nulo} ocorrências com severity nulo")
write_silver(df_ocorrencias, "silver_atendimento_ocorrencias")

elapsed = round((datetime.datetime.utcnow() - t0).total_seconds(), 1)
print(f"04_silver_json_sources concluído em {elapsed}s")
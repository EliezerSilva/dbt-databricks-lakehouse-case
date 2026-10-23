# Databricks notebook source
# DBTITLE 1,No CE pode ser necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

_t0 = datetime.datetime.utcnow()

# COMMAND ----------

cab = (
    spark.read.table(f"{SILVER_SCHEMA}.silver_pedidos_cabecalho")
    .drop("processing_timestamp")
)

itens = (
    spark.read.table(f"{SILVER_SCHEMA}.silver_pedidos_itens")
    .drop("processing_timestamp")
)
logistica = spark.read.table(f"{SILVER_SCHEMA}.silver_logistica")
ocorrencias = spark.read.table(f"{SILVER_SCHEMA}.silver_atendimento_ocorrencias")

dim_cliente = spark.read.table(f"{GOLD_SCHEMA}.dim_cliente")
dim_produto = spark.read.table(f"{GOLD_SCHEMA}.dim_produto")
dim_canal = spark.read.table(f"{GOLD_SCHEMA}.dim_canal")
dim_regiao = spark.read.table(f"{GOLD_SCHEMA}.dim_regiao")
dim_vendedor = spark.read.table(f"{GOLD_SCHEMA}.dim_vendedor")
dim_data = spark.read.table(f"{GOLD_SCHEMA}.dim_data")

# COMMAND ----------

pedidos_joined = itens.join(cab, on="order_id", how="left")


orfaos = pedidos_joined.filter(F.col("status_order").isNull())
n_orfaos = orfaos.count()
if n_orfaos > 0:
    write_orfaos(
        orfaos,
        "fato_pedidos",
        "order_id",
        "order_id"
    )

    log_quality(
        "fato_pedidos",
        "itens_sem_cabecalho",
        n_orfaos,
        f"itens com order_id sem cabeçalho — isolados em {SILVER_SCHEMA}.tabelas_orfas"
    )

pedidos = pedidos_joined.filter(F.col("status_order").isNotNull())

# COMMAND ----------

ocorr_agg = (
    ocorrencias
    .groupBy("order_id")
    .agg(
        F.count("ticket_id").alias("qtd_ocorrencias"),
        F.max(F.when(F.col("event_type") == "cancel_request", 1).otherwise(0)).alias("flag_cancel_request"),
        F.max(F.when(F.col("event_type").isin("refund", "troca"), 1).otherwise(0)).alias("flag_devolucao"),
    )
    .withColumn("flag_tem_ocorrencia", F.lit(1))
)

# COMMAND ----------

ent_dedup = Window.partitionBy("order_ref").orderBy(F.col("delivered_at").desc_nulls_last())

logistica_dedup = (
    logistica
    .withColumn("rn", F.row_number().over(ent_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("order_ref"),
        F.col("delivery_id"),
        F.col("delivery_status"),
        F.col("carrier_name"),
        F.col("carrier_mode"),
        F.col("shipped_at"),
        F.col("delivered_at"),
        F.col("cost").alias("custo_entrega"),
        F.col("flag_atrasado"),
        F.col("dias_atraso"),
        F.col("tempo_entrega_dias"),
    )
)

# COMMAND ----------

fato_base = (
    pedidos
    .join(logistica_dedup, pedidos.order_id == logistica_dedup.order_ref, how="left")
    .join(ocorr_agg, on="order_id", how="left")
    .withColumn("flag_tem_ocorrencia",  F.coalesce(F.col("flag_tem_ocorrencia"),  F.lit(0)))
    .withColumn("qtd_ocorrencias",      F.coalesce(F.col("qtd_ocorrencias"),      F.lit(0)))
    .withColumn("flag_cancel_request",  F.coalesce(F.col("flag_cancel_request"),  F.lit(0)))
    .withColumn("flag_devolucao",       F.coalesce(F.col("flag_devolucao"),        F.lit(0)))
    .withColumn("flag_atrasado",        F.coalesce(F.col("flag_atrasado"),         F.lit(0)))
    .withColumn("flag_cancelado",
        F.when(
            (F.upper(F.col("status_order")) == "CANCELADO") |
            (F.col("item_status") == "CANCELADO"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

# COMMAND ----------

vend_ref = dim_vendedor.select("seller_id", "sk_vendedor", "canal_id", "regional_code")

fato_com_sk = (
    fato_base
    .join(dim_cliente.select("customer_id", "sk_cliente"),
          fato_base.customer_code == dim_cliente.customer_id, how="left")
    .join(dim_produto.select("product_id", "sk_produto"),
          fato_base.product_code == dim_produto.product_id, how="left")
    .join(vend_ref, on="seller_id", how="left")
    .join(dim_canal.select("canal_id", "sk_canal"),
          vend_ref.canal_id == dim_canal.canal_id, how="left")
    .join(dim_regiao.select("regional_code", "sk_regiao"),
          vend_ref.regional_code == dim_regiao.regional_code, how="left")
    .join(
        dim_data.select(F.col("sk_data").alias("sk_data_pedido"), F.col("data").alias("_dt_pedido")),
        fato_base.order_date == F.col("_dt_pedido"), how="left"
    )
    .join(
        dim_data.select(F.col("sk_data").alias("sk_data_prevista"), F.col("data").alias("_dt_prevista")),
        fato_base.promised_date == F.col("_dt_prevista"), how="left"
    )
)

# COMMAND ----------

fato_pedidos = (
    fato_com_sk
    .withColumn("receita_liquida_item",
        F.when(
            F.col("gross_amount").isNotNull() & (F.col("gross_amount") > 0),
            F.round(
                F.col("total_item") - (
                    F.coalesce(F.col("discount_amount"), F.lit(0)) *
                    F.col("total_item") / F.col("gross_amount")
                ), 2
            )
        ).otherwise(F.col("total_item"))
    )
    .select(
        F.col("order_id"),
        F.col("item_seq"),
        F.col("sk_cliente"),
        F.col("sk_produto"),
        F.col("sk_canal"),
        F.col("sk_regiao"),
        F.col("sk_vendedor"),
        F.col("sk_data_pedido"),
        F.col("sk_data_prevista"),
        F.col("quantity").alias("quantidade"),
        F.col("unit_price").alias("valor_unitario"),
        F.col("total_item").alias("valor_item"),
        F.col("receita_liquida_item"),
        F.col("gross_amount").alias("valor_bruto_pedido"),
        F.col("discount_amount").alias("valor_desconto_pedido"),
        F.col("net_amount").alias("valor_liquido_pedido"),
        F.col("custo_entrega"),
        F.col("tempo_entrega_dias"),
        F.col("status_order"),
        F.col("item_status"),
        F.col("delivery_status").alias("status_entrega"),
        F.col("flag_cancelado"),
        F.col("flag_atrasado"),
        F.col("dias_atraso"),
        F.col("flag_tem_ocorrencia"),
        F.col("qtd_ocorrencias"),
        F.col("flag_cancel_request"),
        F.col("flag_devolucao"),
        F.col("order_date").alias("data_pedido"),
        F.col("promised_date").alias("data_prevista_entrega"),
        F.col("shipped_at").alias("data_envio"),
        F.col("delivered_at").alias("data_entrega_real"),
        F.col("order_source"),
        F.current_timestamp().alias("processing_timestamp"),
    )
)

# COMMAND ----------

(
    fato_pedidos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("status_order")
    .saveAsTable(f"{GOLD_SCHEMA}.fato_pedidos")
)

total = fato_pedidos.count()
elapsed = round((datetime.datetime.utcnow() - _t0).total_seconds(), 1)
print(f"{GOLD_SCHEMA}.fato_pedidos: {total} rows | {elapsed}s")

# COMMAND ----------

metricas_integridade = fato_pedidos.agg(
    F.sum(
        F.when(F.col("sk_produto").isNull(), 1).otherwise(0)
    ).alias("sem_produto"),

    F.sum(
        F.when(F.col("sk_cliente").isNull(), 1).otherwise(0)
    ).alias("sem_cliente"),

    F.sum(
        F.when(F.col("sk_regiao").isNull(), 1).otherwise(0)
    ).alias("sem_regiao"),

    F.sum(
        F.when(F.col("sk_canal").isNull(), 1).otherwise(0)
    ).alias("sem_canal"),
).first()

sem_produto = metricas_integridade["sem_produto"]
sem_cliente = metricas_integridade["sem_cliente"]
sem_regiao = metricas_integridade["sem_regiao"]
sem_canal = metricas_integridade["sem_canal"]

log_quality("fato_pedidos", "itens_sem_sk_produto", sem_produto,
            f"{sem_produto} itens com product_code sem correspondência em dim_produto")
log_quality("fato_pedidos", "itens_sem_sk_cliente", sem_cliente,
            f"{sem_cliente} itens com customer_code sem correspondência em dim_cliente")

print(f"""
Integridade referencial:
  sem sk_cliente : {sem_cliente}
  sem sk_produto : {sem_produto}
  sem sk_regiao  : {sem_regiao}
  sem sk_canal   : {sem_canal}
""")

# COMMAND ----------

print("fato_base:", fato_base.count())

print(
    "fato_base distinct:",
    fato_base.select("order_id", "item_seq").distinct().count()
)

print("fato_com_sk:", fato_com_sk.count())

print(
    "fato_com_sk distinct:",
    fato_com_sk.select("order_id", "item_seq").distinct().count()
)
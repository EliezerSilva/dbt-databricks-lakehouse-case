# Databricks notebook source
# MAGIC %run ../00_setup

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

fato = spark.read.table(f"{GOLD_SCHEMA}.fato_pedidos")
dim_data = spark.read.table(f"{GOLD_SCHEMA}.dim_data")
dim_cliente = spark.read.table(f"{GOLD_SCHEMA}.dim_cliente")
dim_produto = spark.read.table(f"{GOLD_SCHEMA}.dim_produto")
dim_canal = spark.read.table(f"{GOLD_SCHEMA}.dim_canal")
dim_regiao = spark.read.table(f"{GOLD_SCHEMA}.dim_regiao")
dim_vendedor = spark.read.table(f"{GOLD_SCHEMA}.dim_vendedor")

fato_enriquecido = (
    fato
    .join(
    dim_data.select("sk_data", "ano_mes", "ano", "mes", "trimestre"),
    on=fato.sk_data_pedido == dim_data.sk_data,
    how="left"
        )
        .drop(dim_data.sk_data)
    .join(dim_regiao.select("sk_regiao", "nome_regiao"),   on="sk_regiao",  how="left")
    .join(dim_canal.select("sk_canal", "nome_canal", "tipo_canal"),     on="sk_canal",   how="left")
    .join(dim_produto.select("sk_produto", "categoria", "subcategoria"), on="sk_produto", how="left")
    .join(dim_cliente.select("sk_cliente", "segmento", "porte"),         on="sk_cliente", how="left")
)

fato_ativo = fato_enriquecido.filter(F.col("flag_cancelado") == 0)

# COMMAND ----------

def write_gold_metric(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.{table_name}")
    )
    print(f"gold.{table_name}: {df.count()} rows")

# COMMAND ----------

metricas_periodo = (
    fato_ativo
    .groupBy("ano", "mes", "ano_mes", "trimestre")
    .agg(
        F.countDistinct("order_id").alias("total_pedidos"),
        F.sum("valor_item").alias("receita_bruta"),
        F.sum("receita_liquida_item").alias("receita_liquida"),
        F.sum("valor_desconto_pedido").alias("total_descontos"),
        F.avg("valor_liquido_pedido").alias("ticket_medio"),
        F.sum("flag_atrasado").alias("total_atrasos"),
        F.avg(F.when(F.col("dias_atraso") > 0, F.col("dias_atraso"))).alias("media_dias_atraso"),
    )
    .orderBy("ano", "mes")
)

write_gold_metric(metricas_periodo, "metricas_por_periodo")

# COMMAND ----------

metricas_regiao = (
    fato_ativo
    .groupBy("sk_regiao", "nome_regiao")
    .agg(
        F.countDistinct("order_id").alias("total_pedidos"),
        F.sum("valor_item").alias("receita_bruta"),
        F.sum("receita_liquida_item").alias("receita_liquida"),
        F.sum("flag_atrasado").alias("total_atrasos"),
        F.countDistinct("sk_cliente").alias("total_clientes_ativos"),
        F.avg("valor_liquido_pedido").alias("ticket_medio"),
        F.sum("flag_tem_ocorrencia").alias("total_ocorrencias"),
        F.avg(F.when(F.col("tempo_entrega_dias").isNotNull(), F.col("tempo_entrega_dias"))).alias("tempo_medio_entrega_dias"),
    )
)

write_gold_metric(metricas_regiao, "metricas_por_regiao")

# COMMAND ----------

metricas_canal = (
    fato_ativo
    .groupBy("sk_canal", "nome_canal", "tipo_canal")
    .agg(
        F.countDistinct("order_id").alias("total_pedidos"),
        F.sum("valor_item").alias("receita_bruta"),
        F.sum("receita_liquida_item").alias("receita_liquida"),
        F.avg("valor_liquido_pedido").alias("ticket_medio"),
        F.countDistinct("sk_cliente").alias("total_clientes"),
        F.sum("flag_atrasado").alias("total_atrasos"),
        F.sum("flag_devolucao").alias("total_devolucoes"),
        F.avg(F.when(F.col("tempo_entrega_dias").isNotNull(), F.col("tempo_entrega_dias"))).alias("tempo_medio_entrega_dias"),
    )
)

write_gold_metric(metricas_canal, "metricas_por_canal")

# COMMAND ----------

metricas_categoria = (
    fato_ativo
    .groupBy("categoria", "subcategoria")
    .agg(
        F.sum("quantidade").alias("total_quantidade"),
        F.sum("valor_item").alias("receita_bruta"),
        F.sum("receita_liquida_item").alias("receita_liquida"),
        F.countDistinct("order_id").alias("total_pedidos"),
        F.avg("valor_unitario").alias("preco_medio_vendido"),
    )
)

write_gold_metric(metricas_categoria, "metricas_por_categoria")

# COMMAND ----------

metricas_operacionais = (
    fato_enriquecido
    .groupBy("ano", "mes", "ano_mes", "nome_regiao", "nome_canal")
    .agg(
        F.countDistinct("order_id").alias("total_pedidos"),
        F.countDistinct(F.when(F.col("flag_cancelado") == 1, F.col("order_id"))).alias("total_cancelados"),
        F.countDistinct(F.when(F.col("flag_atrasado") == 1, F.col("order_id"))).alias("total_atrasados"),
        F.sum("qtd_ocorrencias").alias("total_ocorrencias"),
        F.sum("flag_cancel_request").alias("total_cancel_requests"),
        F.sum("flag_devolucao").alias("total_devolucoes"),
        F.avg(F.when(F.col("tempo_entrega_dias").isNotNull(), F.col("tempo_entrega_dias"))).alias("tempo_medio_entrega_dias"),
    )
    .withColumn(
        "taxa_cancelamento_pct",
        F.when(
            F.col("total_pedidos") > 0,
            F.round(
                F.col("total_cancelados") / F.col("total_pedidos") * 100,
                2
            )
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "taxa_atraso_pct",
        F.when(
            F.col("total_pedidos") > 0,
            F.round(
                F.col("total_atrasados") / F.col("total_pedidos") * 100,
                2
            )
        ).otherwise(F.lit(0))
    )

)

write_gold_metric(metricas_operacionais, "metricas_operacionais")

# COMMAND ----------

print("métricas geradas com sucesso")
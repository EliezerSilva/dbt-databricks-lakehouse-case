# Databricks notebook source
# DBTITLE 1,No CE pode ser necessário executar manualmente antes do `Run all`.
# MAGIC %run ../00_setup

# COMMAND ----------

from pyspark.sql import functions as F
import datetime

# COMMAND ----------

def write_gold(df, table_name):
    t = datetime.datetime.utcnow()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.{table_name}")
    )
    elapsed = round((datetime.datetime.utcnow() - t).total_seconds(), 1)
    n = df.count()
    print(f"gold.{table_name}: {n} rows | {elapsed}s")

# COMMAND ----------

df_clientes = spark.read.table(f"{SILVER_SCHEMA}.silver_clientes")

dim_cliente = (
    df_clientes
    .select(
        F.sha2(F.col("customer_id"), 256).alias("sk_cliente"),
        F.col("customer_id"),
        F.col("nome_cliente"),
        F.col("segmento"),
        F.col("porte"),
        F.col("cidade"),
        F.col("estado"),
        F.col("email"),
        F.col("data_cadastro"),
        F.col("status_cliente"),
    )
    .dropDuplicates(["customer_id"])
)

write_gold(dim_cliente, "dim_cliente")

# COMMAND ----------

df_produtos = spark.read.table(f"{SILVER_SCHEMA}.silver_produtos")

dim_produto = (
    df_produtos
    .select(
        F.sha2(F.col("product_id"), 256).alias("sk_produto"),
        F.col("product_id"),
        F.col("product_name").alias("nome_produto"),
        F.col("category").alias("categoria"),
        F.col("subcategory").alias("subcategoria"),
        F.col("family").alias("familia"),
        F.col("list_price").alias("preco_lista"),
        F.col("currency"),
        F.col("tags_csv"),
        F.col("status"),
    )
    .dropDuplicates(["product_id"])
)

write_gold(dim_produto, "dim_produto")

# COMMAND ----------

df_canais = spark.read.table(f"{SILVER_SCHEMA}.silver_canais")

dim_canal = (
    df_canais
    .select(
        F.sha2(F.col("id_canal"), 256).alias("sk_canal"),
        F.col("id_canal").alias("canal_id"),
        F.col("nome_canal"),
        F.col("tipo_canal"),
        F.col("ativo"),
    )
    .dropDuplicates(["canal_id"])
)

write_gold(dim_canal, "dim_canal")

# COMMAND ----------

df_regioes = spark.read.table(f"{SILVER_SCHEMA}.silver_regioes")

dim_regiao = (
    df_regioes
    .select(
        F.sha2(F.col("regional_code"), 256).alias("sk_regiao"),
        F.col("regional_code"),
        F.col("regional_name").alias("nome_regiao"),
        F.col("state").alias("estado_sede"),
        F.col("manager_name").alias("gestor"),
    )
    .dropDuplicates(["regional_code"])
)

write_gold(dim_regiao, "dim_regiao")

# COMMAND ----------

df_vend = spark.read.table(f"{SILVER_SCHEMA}.silver_vendedores")

dim_vendedor = (
    df_vend
    .select(
        F.sha2(F.col("seller_id"), 256).alias("sk_vendedor"),
        F.col("seller_id"),
        F.col("seller_name").alias("nome_vendedor"),
        F.col("canal_id"),
        F.col("regional_code"),
        F.col("hire_date").alias("data_admissao"),
        F.col("status"),
    )
    .dropDuplicates(["seller_id"])
)

write_gold(dim_vendedor, "dim_vendedor")

# COMMAND ----------

df_pedidos_ref = spark.read.table(f"{SILVER_SCHEMA}.silver_pedidos_cabecalho")
df_entregas_ref = spark.read.table(f"{SILVER_SCHEMA}.silver_logistica")

dates_union = (
    df_pedidos_ref.select(F.col("order_date").alias("dt"))
    .union(
        df_pedidos_ref.select(F.col("promised_date").alias("dt"))
    )
    .union(
        df_entregas_ref.select(
            F.col("shipped_at").cast("date").alias("dt")
        )
    )
    .union(
        df_entregas_ref.select(
            F.col("delivered_at").cast("date").alias("dt")
        )
    )
    .filter(F.col("dt").isNotNull())
    .distinct()
)

bounds   = dates_union.agg(F.min("dt").alias("mn"), F.max("dt").alias("mx")).first()
date_min = (bounds["mn"] - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
date_max = (bounds["mx"] + datetime.timedelta(days=30)).strftime("%Y-%m-%d")

print(f"dim_data range: {date_min} → {date_max}")

date_range = spark.sql(f"""
    SELECT explode(sequence(to_date('{date_min}'), to_date('{date_max}'), interval 1 day)) AS data
""")

MONTHS_PT = {1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
             7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"}
DAYS_PT   = {1:"Segunda",2:"Terça",3:"Quarta",4:"Quinta",5:"Sexta",6:"Sábado",7:"Domingo"}

month_map_expr = F.create_map(*[x for k,v in MONTHS_PT.items() for x in [F.lit(k), F.lit(v)]])
day_map_expr = F.create_map(*[x for k,v in DAYS_PT.items()   for x in [F.lit(k), F.lit(v)]])

dim_data = (
    date_range
    .withColumn("sk_data", F.date_format(F.col("data"), "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year(F.col("data")))
    .withColumn("mes", F.month(F.col("data")))
    .withColumn("dia", F.dayofmonth(F.col("data")))
    .withColumn("trimestre", F.quarter(F.col("data")))
    .withColumn("semana_ano", F.weekofyear(F.col("data")))
    .withColumn("nome_mes", month_map_expr[F.month(F.col("data"))])
    .withColumn("dia_semana_iso",
        F.when(F.dayofweek(F.col("data")) == 1, F.lit(7))
         .otherwise(F.dayofweek(F.col("data")) - 1)
    )
    .withColumn("nome_dia_semana", day_map_expr[
        F.when(F.dayofweek(F.col("data")) == 1, F.lit(7))
         .otherwise(F.dayofweek(F.col("data")) - 1)
    ])
    .withColumn("flag_fim_semana",
        F.when(F.dayofweek(F.col("data")).isin(1, 7), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn("ano_mes", F.date_format(F.col("data"), "yyyy-MM"))
)

write_gold(dim_data, "dim_data")
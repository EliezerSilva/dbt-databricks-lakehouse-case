# Databricks notebook source
# MAGIC %run ../00_setup

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

t0 = datetime.datetime.utcnow()

def write_silver(df, table_name, partition_col=None):
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.saveAsTable(f"{SILVER_SCHEMA}.{table_name}")
    n = df.count()
    print(f"silver.{table_name}: {n} rows")
    return n

def normalize_state(col_name):
    clean = F.regexp_replace(
        F.lower(F.trim(F.col(col_name))),
        r"[\.\-]",
        ""
    )

    return (
        F.when(
            clean.isin(
                "sc",
                "santa catarina",
                "sta catarina",
                "s catarina"
            ),
            F.lit("SC")
        )
         .when(clean.isin("sp", "sao paulo", "são paulo"), F.lit("SP"))
         .when(clean.isin("rj", "rio de janeiro"), F.lit("RJ"))
         .when(clean.isin("mg", "minas gerais"), F.lit("MG"))
         .when(clean.isin("pr", "parana", "paraná"), F.lit("PR"))
         .when(clean.isin("rs", "rio grande do sul"), F.lit("RS"))
         .when(clean.isin("go", "goias", "goiás"), F.lit("GO"))
         .when(clean.isin("ba", "bahia"), F.lit("BA"))
         .when(clean.isin("ce", "ceara", "ceará"), F.lit("CE"))
         .when(clean.isin("pe", "pernambuco"), F.lit("PE"))
         .when(clean.isin("am", "amazonas"), F.lit("AM"))
         .otherwise(F.upper(F.trim(F.col(col_name))))
    )

# COMMAND ----------

df_regioes_raw = spark.read.table(f"{BRONZE_SCHEMA}.bronze_legado_regioes")


df_invalidos_reg = df_regioes_raw.filter(
    (F.col("active_flag") == "0") | F.col("regional_name").isNull()
)

n_invalidos_reg = df_invalidos_reg.count()

if n_invalidos_reg > 0:
    write_invalidos(df_invalidos_reg, "silver_regioes", "active_flag=0 ou regional_name nulo")

    log_quality(
        "silver_regioes",
        "regioes_inativas_descartadas",
        n_invalidos_reg,
        "regiões sem nome ou inativas descartadas"
    )

regioes_dedup_window = Window.partitionBy("regional_code").orderBy(
    F.when(F.col("state") == "SP", 0).otherwise(1)
)

df_regioes = (
    df_regioes_raw
    .filter(F.col("active_flag") == "1")
    .filter(F.col("regional_name").isNotNull())
    .withColumn("regional_code",
        F.when(F.upper(F.trim(F.col("regional_code"))) == "SUL", F.lit("S"))
         .otherwise(F.upper(F.trim(F.col("regional_code"))))
    )
    .withColumn("rn", F.row_number().over(regioes_dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "active_flag", "source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

write_silver(df_regioes, "silver_regioes")

# COMMAND ----------

df_canais = (
    spark.read.table(f"{BRONZE_SCHEMA}.bronze_comercial_canais")
    .withColumn("ativo",
        F.when(F.lower(F.trim(F.col("ativo"))) == "sim", F.lit(True))
         .when(F.lower(F.trim(F.col("ativo"))) == "nao", F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )
    .withColumn("tipo_canal", F.initcap(F.lower(F.trim(F.col("tipo_canal")))))
    .withColumn("nome_canal", F.trim(F.col("nome_canal")))
    .drop("source_file", "processing_timestamp", "ingestion_date", "observacao")
    .withColumn("processing_timestamp", F.current_timestamp())
)

write_silver(df_canais, "silver_canais")

# COMMAND ----------

df_vend_raw = spark.read.table(f"{BRONZE_SCHEMA}.bronze_comercial_vendedores")

n_regional_nok = df_vend_raw.filter(
    F.lower(F.trim(F.col("regional_code"))).isin("sul") | F.col("regional_code").isNull()
).count()
log_quality("silver_vendedores", "regional_code_normalizado", n_regional_nok,
            f"{n_regional_nok} vendedores com regional_code inválido/nulo")

df_vendedores = (
    df_vend_raw
    .withColumn("status",
        F.when(F.col("status").isNull(), "NAO_INFORMADO")
         .otherwise(F.initcap(F.lower(F.trim(F.col("status")))))
    )
    .withColumn("regional_code",
        F.when(F.lower(F.trim(F.col("regional_code"))) == "sul", F.lit("S"))
         .when(F.col("regional_code").isNull(), F.lit("NAO_INFORMADO"))
         .otherwise(F.upper(F.trim(F.col("regional_code"))))
    )
    .withColumn(
        "hire_date",
        F.coalesce(
            F.try_to_timestamp(F.col("hire_date"), F.lit("yyyy-MM-dd")).cast("date"),
            F.try_to_timestamp(F.col("hire_date"), F.lit("dd/MM/yyyy")).cast("date"),
        )
    )
    .drop("source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

write_silver(df_vendedores, "silver_vendedores")

# COMMAND ----------

df_clientes = (
    spark.read.table(f"{BRONZE_SCHEMA}.bronze_crm_clientes")
    .withColumn("estado", normalize_state("estado"))
    .withColumn("porte", F.initcap(F.lower(F.trim(F.col("porte")))))
    .withColumn("segmento", F.initcap(F.lower(F.trim(F.col("segmento")))))
    .withColumn("status_cliente",
        F.when(F.col("status_cliente").isNull(), "NAO_INFORMADO")
         .otherwise(F.initcap(F.lower(F.trim(F.col("status_cliente")))))
    )
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn(
        "data_cadastro",
        F.coalesce(
            F.try_to_timestamp(F.col("data_cadastro"), F.lit("yyyy-MM-dd")).cast("date"),
            F.try_to_timestamp(F.col("data_cadastro"), F.lit("yyyy/MM/dd")).cast("date"),
            F.try_to_timestamp(F.col("data_cadastro"), F.lit("dd/MM/yyyy")).cast("date"),
        )
    )
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
    .drop("source_file", "processing_timestamp", "ingestion_date")
    .withColumn("processing_timestamp", F.current_timestamp())
)

write_silver(df_clientes, "silver_clientes")

elapsed = round((datetime.datetime.utcnow() - t0).total_seconds(), 1)
print(f"silver cadastros loaded | {elapsed}s")
# Databricks notebook source
# MAGIC %run ../00_runtime_config

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

sources = [
    ("crm_clientes_export.xlsx", "bronze_crm_clientes"),
    ("comercial_canais.xlsx", "bronze_comercial_canais"),
    ("vendedores.csv", "bronze_comercial_vendedores"),
    ("legado_regioes_pipe.txt", "bronze_legado_regioes"),
]

assert CATALOG == "workspace"
assert BRONZE_SCHEMA == "bronze_dev"
assert CONTROL_SCHEMA == "control_dev"

log_table = f"{CATALOG}.{CONTROL_SCHEMA}.ingestion_log"
assert spark.catalog.tableExists(log_table)

for filename, table_name in sources:
    print(f"\\n=== {filename} ===")

    path = f"{SOURCE_ROOT}/{filename}"
    files = dbutils.fs.ls(path)

    assert len(files) == 1
    assert files[0].name.rstrip("/") == filename
    assert not files[0].isDir()
    assert files[0].size > 0

    print("Size:", files[0].size)
    print("Modified:", files[0].modificationTime)

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

    count = history.count()

    print("Target:", target)
    print("Exists:", exists)
    print("History rows:", count)

    if filename.endswith(".xlsx"):
        print("Testing Excel read...")
        pdf = pd.read_excel(path, dtype=str, nrows=3)
        print("Excel columns:", len(pdf.columns))
        print("Excel sample rows:", len(pdf))

    assert not exists, f"Tabela preexistente: {target}"
    assert count == 0, f"Histórico preexistente: {filename}"


print("=== DATAFRAME VALIDATION ===")

for filename, table_name in sources:
    path = f"{SOURCE_ROOT}/{filename}"

    if filename.endswith(".xlsx"):
        pdf = pd.read_excel(path, dtype=str)
        assert not pdf.empty, f"Excel vazio: {filename}"

        df = spark.createDataFrame(pdf)
        assert df.count() == len(pdf)

    elif filename == "vendedores.csv":
        raw = dbutils.fs.head(path, 2048)
        header = raw.splitlines()[0].strip()
        print("DAB_HEADER_DIAGNOSTIC")
        print("File:", filename)
        print("Header repr:", repr(header))
        print("Semicolon count:", header.count(";"))
        print("Comma count:", header.count(","))
        print("Pipe count:", header.count("|"))
        print("Detected columns:", len(header.split(";")))
        assert len(header.split(";")) == 6, (
            f"{filename}: esperado 6 colunas; "
            f"encontradas {len(header.split(';'))}"
        )

        df = (
            spark.read
            .option("header", "true")
            .option("sep", ";")
            .csv(path)
        )

    else:
        raw = dbutils.fs.head(path, 2048)
        header = raw.splitlines()[0].strip()
        assert len(header.split("|")) == 5

        df = (
            spark.read
            .option("header", "true")
            .option("sep", "|")
            .csv(path)
        )

    rows = df.count()
    assert rows > 0, f"Fonte vazia: {filename}"

    print(
        f"{filename}: rows={rows}, "
        f"columns={len(df.columns)}"
    )

print("DAB_CADASTROS_DATAFRAME_VALIDATED")

print("\\nDAB_CADASTROS_PREFLIGHT_SUCCESS")

# Databricks notebook source
storage_account = dbutils.secrets.get("kv-scope", "storage-account-name")
access_key = dbutils.secrets.get("kv-scope", "storage-account-key")

print(storage_account)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://bronze@sdpprostorage.dfs.core.windows.net/"
    )
)

# COMMAND ----------

customers_df = spark.read.csv(
    "abfss://bronze@sdpprostorage.dfs.core.windows.net/customers.csv",
    header=True,
    inferSchema=True
)

orders_df = spark.read.csv(
    "abfss://bronze@sdpprostorage.dfs.core.windows.net/orders.csv",
    header=True,
    inferSchema=True
)

payments_df = spark.read.csv(
    "abfss://bronze@sdpprostorage.dfs.core.windows.net/payments.csv",
    header=True,
    inferSchema=True
)

products_df = spark.read.csv(
    "abfss://bronze@sdpprostorage.dfs.core.windows.net/products.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

customers_df.show(5)

# COMMAND ----------

customers_bronze.show(5)
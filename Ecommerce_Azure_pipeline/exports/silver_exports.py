# Databricks notebook source
spark.read.table("customers_silver") \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://silver@sdpprostorage.dfs.core.windows.net/customers/")

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://silver@sdpprostorage.dfs.core.windows.net/"
    )
)

# COMMAND ----------

tables = [
    "customers_silver",
    "orders_silver",
    "payments_silver",
    "products_silver"
]

for table in tables:
    folder_name = table.replace("_silver", "")

    (
        spark.read.table(table)
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            f"abfss://silver@sdpprostorage.dfs.core.windows.net/{folder_name}/"
        )
    )

    print(f"Exported {table}")
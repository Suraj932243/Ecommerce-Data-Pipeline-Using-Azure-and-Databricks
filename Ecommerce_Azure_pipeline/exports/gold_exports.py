# Databricks notebook source
display(
    dbutils.fs.ls(
        "abfss://gold@sdpprostorage.dfs.core.windows.net/"
    )
)

# COMMAND ----------

gold_tables = [
    "revenue_by_city_gold",
    "delivery_performance_gold",
    "product_analysis_gold",
    "payment_analysis_gold",
    "customer_segments_gold"
]

for table in gold_tables:

    folder_name = table.replace("_gold", "")

    (
        spark.read.table(table)
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            f"abfss://gold@sdpprostorage.dfs.core.windows.net/{folder_name}/"
        )
    )

    print(f"{table} exported successfully")
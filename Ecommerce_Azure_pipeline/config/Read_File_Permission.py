# Databricks notebook source
spark.sql("""
GRANT READ FILES
ON EXTERNAL LOCATION bronze_location
TO `account users`
""")

# COMMAND ----------

spark.sql("""
GRANT CREATE EXTERNAL TABLE
ON EXTERNAL LOCATION bronze_location
TO `account users`
""")

# COMMAND ----------

spark.sql("SHOW GRANTS ON EXTERNAL LOCATION bronze_location").show(truncate=False)

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@sdpprostorage.dfs.core.windows.net/")

# COMMAND ----------

spark.sql("SHOW STORAGE CREDENTIALS").show(truncate=False)
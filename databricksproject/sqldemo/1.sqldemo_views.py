# Databricks notebook source
df = spark.read.parquet("abfss://results@covidapp3.dfs.core.windows.net/races")

# COMMAND ----------

df.createOrReplaceTempView("peo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from peo where driver_name = "Alberto Ascari"

# COMMAND ----------

df.createOrReplaceGlobalTempView("pe")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pe where driver_name = "Alberto Ascari"

# COMMAND ----------

# MAGIC %sql
# MAGIC use database demo

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view pr as select * from demo.races

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.pr

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo

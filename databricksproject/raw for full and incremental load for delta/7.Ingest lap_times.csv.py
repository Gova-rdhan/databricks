# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","")
date = dbutils.widgets.get("file_date")

# COMMAND ----------

# %sql
# use processedtb

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("lap",IntegerType(),False),
    StructField("position",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("milliseconds",StringType(),True)])

# COMMAND ----------

df = spark.read.schema(circuits_schema).csv(f"abfss://rawdb@covidapp3.dfs.core.windows.net/{date}/lap_times")

# COMMAND ----------

df1 = df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

incremental_upsert('deltadb','lap_time','delta',df1,'raceId','tgt.raceId = src.raceId')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltadb.lap_time

# COMMAND ----------

# incremental_load(df1,'processeddb','lap_times','driverId')

# COMMAND ----------

# df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/lap_times")

# COMMAND ----------

# df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.lap_times")

# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","")
date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

pit_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("duration", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("milliseconds", StringType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("stop", IntegerType(), True),
    StructField("time", StringType(), True)
])


# COMMAND ----------

df = spark.read.option("multiline",True).schema(pit_schema).json(f"abfss://rawdb@covidapp3.dfs.core.windows.net/{date}/pit_stops.json")

# COMMAND ----------

df1 = df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/pit_stops")

# COMMAND ----------

# df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.pit_stops")

# COMMAND ----------

# incremental_load(df1,'processeddb','pit_stops','raceId')

# COMMAND ----------

display(df1)

# COMMAND ----------

incremental_upsert('deltadb','pit_stopp','delta',df1,'raceId','raceId','tgt.raceId = src.raceId')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltadb.pit_stopp

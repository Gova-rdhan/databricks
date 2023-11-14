# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","")
date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC use processedtb

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

qualify_schema = StructType(fields=[
    StructField("constructorId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("number",IntegerType(),False),
    StructField("position",IntegerType(),True),
    StructField("q1",StringType(),True),
    StructField("q2",StringType(),True),
    StructField("q3",StringType(),True),
    StructField("qualifyId",IntegerType(),False),
    StructField("raceId",IntegerType(),False)])

# COMMAND ----------

df = spark.read.option("multiline",True).schema(qualify_schema).json(f"abfss://raw@covidapp3.dfs.core.windows.net/{date}/qualifying")

# COMMAND ----------

df1 = df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

incremental_load(df1,'processeddb','qualifying','qualifyId')

# COMMAND ----------

# df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/qualifying")

# COMMAND ----------

# df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.qualifying")

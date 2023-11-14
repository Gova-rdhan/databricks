# Databricks notebook source
# MAGIC %sql
# MAGIC use processedtb

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

df = spark.read.schema(circuits_schema).csv("abfss://raw@covidapp3.dfs.core.windows.net/lap_times")

# COMMAND ----------

df1 = df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/lap_times")

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.lap_times")

# Databricks notebook source
# MAGIC %sql
# MAGIC use processedtb

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

df = spark.read.option("multiline",True).schema(pit_schema).json("abfss://raw@covidapp3.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

df1 = df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/pit_stops")

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.pit_stops")

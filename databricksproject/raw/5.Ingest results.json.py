# Databricks notebook source
# MAGIC %sql
# MAGIC use processedtb

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("time",StringType(),True),
    StructField("statusId",IntegerType(),True),
    StructField("resultId",IntegerType(),True),
    StructField("rank",IntegerType(),True),
    StructField("raceId",IntegerType(),True),
    StructField("positionText",IntegerType(),True),
    StructField("positionOrder",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("points",IntegerType(),True),
    StructField("number",IntegerType(),True),
    StructField("constructorId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("fastestLap",StringType(),True),
    StructField("fastestLapSpeed",FloatType(),True),
    StructField("grid",IntegerType(),True),
    StructField("laps",IntegerType(),True),
    StructField("milliseconds",IntegerType(),True),
    StructField("constructorRef",StringType(),True),
    StructField("name",StringType(),True),
    StructField("nationality",StringType(),True)])

# COMMAND ----------

df = spark.read.schema(results_schema).json("abfss://raw@covidapp3.dfs.core.windows.net/results.json")

# COMMAND ----------

df1 = df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.results")

# COMMAND ----------

df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/results")

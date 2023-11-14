# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("p_filedate","")
v_date = dbutils.widgets.get("p_filedate")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from pyspark.sql.functions import current_timestamp,col,lit

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId",IntegerType(),False),
    StructField("circuitRef",StringType(),True),
    StructField("name",StringType(),True),
    StructField("location",StringType(),True),
    StructField("country",StringType(),True),
    StructField("lat",DoubleType(),True),
    StructField("lng",DoubleType(),True),
    StructField("alt",IntegerType(),True)])

# COMMAND ----------

df = spark.read.schema(circuits_schema).option('header',True).csv(f"abfss://rawdb@covidapp3.dfs.core.windows.net/{v_date}/circuits.csv")

# COMMAND ----------

df1 = df.select(col("circuitId").alias("circuit_id"),col("circuitRef").alias("circuit_ref"),col("name").alias("circuit_name"),\
        col("location").alias("circuit_location"),"country",col("lat").alias("latitude"),col("lng").alias("longitude"),col("alt").alias("altitude"))\
        .withColumn("ingestion_date",current_timestamp())\
        .withColumn("file_date",lit(v_date))

# COMMAND ----------

incremental_load(df1,'processeddb','circuits','circuit_id')

# COMMAND ----------



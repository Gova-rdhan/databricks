# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

display(spark.read.csv("abfss://raw@covidapp3.dfs.core.windows.net/circuits.csv"))

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

df = spark.read.schema(circuits_schema).option('header',True).csv("abfss://raw@covidapp3.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

df1 = df.select(col("circuitId").alias("circuit_id"),col("circuitRef").alias("circuit_ref"),col("name").alias("circuit_name"),\
        col("location").alias("circuit_location"),"country",col("lat").alias("latitude"),col("lng").alias("longitude"),col("alt").alias("altitude"))\
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists presentationdtb
# MAGIC location "abfss://presentationdtb@covidapp3.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC use presentationdtb

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from processedtb.circuits

# COMMAND ----------

display(spark.read.parquet("abfss://processed@covidapp3.dfs.core.windows.net/circuits"))

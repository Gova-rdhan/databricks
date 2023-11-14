# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("p_filedate","")
v_date = dbutils.widgets.get("p_filedate")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,TimestampType
from pyspark.sql.functions import current_timestamp,concat,lit,col,to_timestamp

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("year",IntegerType(),True),
    StructField("round",IntegerType(),True),
    StructField("circuitid",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("date",StringType(),True),
    StructField("time",StringType(),True),
    StructField("url",StringType(),True)])

# COMMAND ----------

df = spark.read.schema(races_schema).option('header',True).csv(f"abfss://rawdb@covidapp3.dfs.core.windows.net/{v_date}/races.csv")

# COMMAND ----------

df1 = df.select(df.raceId.alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitid").alias("circuit_id"),col("name").alias("race_name"),col("date"),col("time"))\
        .withColumn("ingestion_date",current_timestamp())\
        .withColumn("file_date",lit(v_date))\
        .withColumn("race_timestamp",to_timestamp(concat(df.date,lit(' '),df.time)))

# COMMAND ----------

incremental_load(df1,'processeddb','racess','race_id')

# Databricks notebook source
# MAGIC %sql
# MAGIC use processedtb

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,TimestampType
from pyspark.sql.functions import current_timestamp,concat,lit,col,to_timestamp

# COMMAND ----------

display(spark.read.csv("abfss://raw@covidapp3.dfs.core.windows.net/races.csv"))

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

df = spark.read.schema(races_schema).option('header',True).csv("abfss://raw@covidapp3.dfs.core.windows.net/races.csv")

# COMMAND ----------

df1 = df.select(df.raceId.alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitid").alias("circuit_id"),col("name").alias("race_name"),col("date"),col("time"))\
        .withColumn("ingestion_date",current_timestamp())\
        .withColumn("race_timestamp",to_timestamp(concat(df.date,lit(' '),df.time)))

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.races")

# COMMAND ----------

df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/races")
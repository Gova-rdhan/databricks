# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("p_filedate","2021-03-21")
v_date = dbutils.widgets.get("p_filedate")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp,col,lit

# COMMAND ----------

constructors_schema = StructType(fields=[
    StructField("constructorId",IntegerType(),False),
    StructField("constructorRef",StringType(),True),
    StructField("name",StringType(),True),
    StructField("nationality",StringType(),True)])

# COMMAND ----------

df = spark.read.schema(constructors_schema).json(f"abfss://rawdb@covidapp3.dfs.core.windows.net/{v_date}/constructors.json")

# COMMAND ----------

df1 = df.select(col("constructorId").alias("constructor_id"),
                col("constructorRef").alias("constructor_ref"),
                col("name").alias("constructor_name"),
                col("nationality"))\
        .withColumn("file_date",lit(v_date))\
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.format("delta").mode("overwrite").saveAsTable("deltadb.constructors")

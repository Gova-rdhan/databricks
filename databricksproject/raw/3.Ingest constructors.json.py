# Databricks notebook source
# MAGIC %sql 
# MAGIC use processedtb

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

constructors_schema = StructType(fields=[
    StructField("constructorId",IntegerType(),False),
    StructField("constructorRef",StringType(),True),
    StructField("name",StringType(),True),
    StructField("nationality",StringType(),True)])

# COMMAND ----------

df = spark.read.schema(constructors_schema).json("abfss://raw@covidapp3.dfs.core.windows.net/constructors.json")

# COMMAND ----------

df1 = df.select(col("constructorId").alias("constructor_id"),
                col("constructorRef").alias("constructor_ref"),
                col("name").alias("constructor_name"),
                col("nationality"))\
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.constructors")

# COMMAND ----------

df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/constructors")

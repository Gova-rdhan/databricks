# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("p_filedate","2021-03-21")
v_date = dbutils.widgets.get("p_filedate")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql.functions import current_timestamp,col,lit,concat

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename",StringType(),True),
    StructField("surname",StringType(),True)
])

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

drivers_schema = StructType(fields=[
    StructField("code", StringType(), False),
    StructField("dob", DateType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("number", IntegerType(), False),
    StructField("driverRef", StringType(), False),
    StructField("name",name_schema),
    StructField("nationality", StringType(), True)
])


# COMMAND ----------

df = spark.read.schema(drivers_schema).json(f"abfss://rawdb@covidapp3.dfs.core.windows.net/{v_date}/drivers.json")

# COMMAND ----------

df1 = df.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
        .withColumn("ingestion_date",current_timestamp())\
        .withColumn("file_date",lit(v_date))

# COMMAND ----------

df1.write.format("delta").mode("overwrite").saveAsTable("deltadb.drivers")

# COMMAND ----------



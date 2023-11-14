# Databricks notebook source
# MAGIC %sql
# MAGIC use processedtb

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

df = spark.read.schema(drivers_schema).json("abfss://raw@covidapp3.dfs.core.windows.net/drivers.json")

# COMMAND ----------

df1 = df.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").saveAsTable("processedtb.drivers")

# COMMAND ----------

df1.write.mode("overwrite").parquet("abfss://processed@covidapp3.dfs.core.windows.net/drivers")

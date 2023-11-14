# Databricks notebook source
# MAGIC %sql 
# MAGIC use presentationdtb

# COMMAND ----------

df = spark.read.parquet("abfss://results@covidapp3.dfs.core.windows.net/race_results")

# COMMAND ----------

from pyspark.sql.functions import count,col,when,sum

# COMMAND ----------

df1 = df.groupBy("driver_name","race_year","nationality","constructor_name").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc
ff = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
d = df1.withColumn("rank",rank().over(ff))
d.write.mode("overwrite").parquet("abfss://results@covidapp3.dfs.core.windows.net/driver_standins")

# COMMAND ----------

d.write.mode("overwrite").format("parquet").saveAsTable("presentationdtb.driver_standins")

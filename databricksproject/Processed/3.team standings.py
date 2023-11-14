# Databricks notebook source
# MAGIC %sql
# MAGIC use presentationdtb

# COMMAND ----------

df = spark.read.parquet("abfss://results@covidapp3.dfs.core.windows.net/race_results")

# COMMAND ----------

from pyspark.sql.functions import when,col,sum,count
ff = df.groupBy("constructor_name","race_year").agg(
    sum("points").alias("total_points"),count(when(col("position") == 1,True)).alias("wins")
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
dr = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
dd=ff.withColumn("rank",rank().over(dr))
dd.write.mode("overwrite").parquet("abfss://results@covidapp3.dfs.core.windows.net/team_standings")

# COMMAND ----------

dd.write.mode("overwrite").format("parquet").saveAsTable("presentationdtb.team_standings")

# Databricks notebook source
df = spark.read.format("delta").load("abfss://delta@covidapp3.dfs.core.windows.net/result")

# COMMAND ----------

from pyspark.sql.functions import sum, min, max, count, countDistinct,desc

# Group by "driver_name" and perform aggregations
agg_df = df.groupBy("driver_name","race_year").agg(
    countDistinct("race_name").alias("total_races"),
    sum("points").alias("total_points")
)

# Select the necessary columns for display
result_df = agg_df.select("driver_name","race_year", "total_races", "total_points").orderBy(desc("total_points"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc
df1 = Window.partitionBy("race_year").orderBy(desc("total_points"))
dis = result_df.withColumn("rank",rank().over(df1))
# dis.write.mode("overwrite").parquet("abfss://results@covidapp3.dfs.core.windows.net/races")

# COMMAND ----------

# dis.write.mode("overwrite").format("parquet").saveAsTable("presentationdtb.races")

# COMMAND ----------

incremental_upsert('delta1db','races','delta1',dis,'raceId','tgt.race_year = src.race_year and tgt.driver_name = src.driver_name')

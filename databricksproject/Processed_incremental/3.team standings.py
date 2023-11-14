# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","")
date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC use presentationdtb

# COMMAND ----------

df = spark.read.parquet("abfss://presentationdb@covidapp3.dfs.core.windows.net/r_results").filter(f"file_date = '{date}'").select("race_year").distinct().collect()

# COMMAND ----------

display(df)

# COMMAND ----------

race = []
for r in df:
    race.append(r.race_year)

# COMMAND ----------

from pyspark.sql.functions import col
df = spark.read.parquet("abfss://presentationdb@covidapp3.dfs.core.windows.net/r_results").filter(col("race_year").isin(race))

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
# dd.write.mode("overwrite").parquet("abfss://results@covidapp3.dfs.core.windows.net/team_standings")

# COMMAND ----------

display(dd)

# COMMAND ----------

# dd.write.mode("overwrite").format("parquet").saveAsTable("presentationdtb.team_standings")

# COMMAND ----------

incremental_load(dd,'presentationdb','team_standings','race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from presentationdb.team_standings

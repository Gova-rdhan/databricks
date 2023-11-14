# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","")
date = dbutils.widgets.get("file_date")

# COMMAND ----------

df = spark.read.format("delta").load("abfss://delta1@covidapp3.dfs.core.windows.net/race_results").filter(f"file_date = '{date}'").select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = []
for r in df:
    race_year_list.append(r.race_year)

# COMMAND ----------

from pyspark.sql.functions import col
df = spark.read.format("delta").load("abfss://delta1@covidapp3.dfs.core.windows.net/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import count,col,when,sum

# COMMAND ----------

df1 = df.groupBy("driver_name","race_year","file_Date","nationality","constructor_name").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc
ff = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
ds = df1.withColumn("rank",rank().over(ff))
# d.write.mode("overwrite").parquet("abfss://results@covidapp3.dfs.core.windows.net/driver_standins")

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_name,race_year,count(1) from delta1db.driv_standings group by driver_name,race_year having count(1) > 1

# COMMAND ----------

incremental_upsert('delta1db','driver_standings','delta1',ds,'race_year','tgt.race_year = src.race_year and tgt.driver_name = src.driver_name')

# COMMAND ----------

# d.write.mode("overwrite").format("parquet").saveAsTable("presentationdtb.driver_standins")

# COMMAND ----------

# incremental_load(ds,'presentationdb','drivers_standing','race_year')

# COMMAND ----------

# %sql
# select * from presentationdb.drivers_standing

# COMMAND ----------

# %sql
# drop table presentationdb.drivers_standing

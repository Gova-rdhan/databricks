# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists presentationdtb
# MAGIC location "abfss://presentationdtb@covidapp3.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC use presentationdtb

# COMMAND ----------

races_df = spark.read.parquet("abfss://processed@covidapp3.dfs.core.windows.net/races")
circuits_df = spark.read.parquet("abfss://processed@covidapp3.dfs.core.windows.net/circuits")
results_df = spark.read.parquet("abfss://processed@covidapp3.dfs.core.windows.net/results")
drivers_df = spark.read.parquet("abfss://processed@covidapp3.dfs.core.windows.net/drivers").withColumnRenamed("name","driver_name")
races_df = spark.read.parquet("abfss://processed@covidapp3.dfs.core.windows.net/races")
constructors_df = spark.read.parquet("abfss://processed@covidapp3.dfs.core.windows.net/constructors")

# COMMAND ----------

df = races_df.join(circuits_df,races_df.circuit_id == circuits_df.circuit_id,"inner")

# COMMAND ----------

final_df = results_df.join(df,results_df.raceId == df.race_id,"inner")\
    .join(drivers_df,results_df.driverId == drivers_df.driverId,"inner")\
    .join(constructors_df,results_df.constructorId == constructors_df.constructor_id,"inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final = final_df.select(races_df.race_name,races_df.race_year,races_df.race_timestamp,circuits_df.circuit_location,drivers_df.driver_name,drivers_df.number,drivers_df.nationality,constructors_df.constructor_name,results_df.grid,results_df.fastestLap,results_df.time,results_df.points,results_df.position).withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

final.write.mode("overwrite").format("parquet").saveAsTable("presentationdtb.race_results")
display(spark.sql("select * from presentationdtb.race_results"))

# COMMAND ----------

final.write.mode("overwrite").parquet("abfss://results@covidapp3.dfs.core.windows.net/race_results")

# COMMAND ----------

final.write.mode("overwrite").format("parquet").saveAsTable("presentationtb.race_results")

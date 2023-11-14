# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","")
date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists presentationdb
# MAGIC location "abfss://presentationdb@covidapp3.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC use presentationdb

# COMMAND ----------

races_df = spark.read.parquet("abfss://processeddb@covidapp3.dfs.core.windows.net/races")
circuits_df = spark.read.parquet("abfss://processeddb@covidapp3.dfs.core.windows.net/circuits")
drivers_df = spark.read.parquet("abfss://processeddb@covidapp3.dfs.core.windows.net/drivers").withColumnRenamed("name","driver_name")
constructors_df = spark.read.parquet("abfss://processeddb@covidapp3.dfs.core.windows.net/constructors")

# COMMAND ----------

results_df = spark.read.parquet("abfss://processeddb@covidapp3.dfs.core.windows.net/results").filter(f"file_date = '{date}'")

# COMMAND ----------

df = races_df.join(circuits_df,races_df.circuit_id == circuits_df.circuit_id,"inner")

# COMMAND ----------

final_df = results_df.join(df,results_df.raceId == df.race_id,"inner")\
    .join(drivers_df,results_df.driverId == drivers_df.driverId,"inner")\
    .join(constructors_df,results_df.constructorId == constructors_df.constructor_id,"inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final = final_df.select(races_df.race_name,races_df.race_year,races_df.race_timestamp,circuits_df.circuit_location,drivers_df.driver_name,drivers_df.number,drivers_df.nationality,constructors_df.constructor_name,results_df.grid,results_df.fastestLap,results_df.time,results_df.points,results_df.position,results_df.raceId,results_df.file_Date).withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# def rearrange_cols(part_col,df_nam):
#     col_list = []
#     for colname in df_nam.schema.names :
#         if colname != part_col:
#             col_list.append(colname)
#     col_list.append(part_col)
#     final = df_nam.select(col_list)
#     return final
# rt = rearrange_cols('raceId',final)
# print(rt)

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# if spark._jsparkSession.catalog().tableExists("presentationdb.r_results"):
#     rt.write.mode("overwrite").insertInto("presentationdb.r_results")
# else:
#     rt.write.mode("overwrite").partitionBy("raceId").format("parquet").saveAsTable("presentationdb.r_results")

# COMMAND ----------

# %sql
# drop table presentationdb.raceresults

# COMMAND ----------

incremental_load(final,'presentationdb','r_results','raceId')

# COMMAND ----------

# display(spark.sql("delete from presentationdb.r_results where file_date = '2021-03-28' and driver_name = 'Nicholas Latifi'"))

# COMMAND ----------

# %sql
# delete from presentationdb.r_results where race_year = '2021'

# COMMAND ----------

# final.write.mode("overwrite").format("parquet").saveAsTable("presentationdb.race_results")
# display(spark.sql("select * from presentationdb.race_results"))

# COMMAND ----------

# final.write.mode("overwrite").parquet("abfss://results@covidapp3.dfs.core.windows.net/race_results")

# COMMAND ----------

# final.write.mode("overwrite").format("parquet").saveAsTable("presentationtb.race_results")

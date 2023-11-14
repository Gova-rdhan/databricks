# Databricks notebook source
df = spark.sql("select * from presentationdb.r_results")

# COMMAND ----------

# %sql
# show tables in presentationdb

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DELTA
# MAGIC LOCATION "abfss://delta@covidapp3.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC use delta

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("DELTA.results1")
#if we want to use partitionby just use .partitionby column name for both tables and file format

# COMMAND ----------

#this bwlow one is for save as file
df.write.format("delta").mode("overwrite").save("abfss://delta@covidapp3.dfs.core.windows.net/circuits_file")

# COMMAND ----------

df2 = spark.read.format("delta").load("abfss://delta@covidapp3.dfs.core.windows.net/circuits_file")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta.results 
# MAGIC using delta
# MAGIC location "abfss://delta@covidapp3.dfs.core.windows.net/circuits_file"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.results1

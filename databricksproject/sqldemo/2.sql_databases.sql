-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

use database demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.parquet("abfss://results@covidapp3.dfs.core.windows.net/races")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").saveAsTable("demo.races")

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

select * from demo.race1 where rank <5

-- COMMAND ----------

describe table extended demo.race1

-- COMMAND ----------

drop table demo.race1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").option("path","abfss://results@covidapp3.dfs.core.windows.net/race2").saveAsTable("demo.race2")

-- COMMAND ----------

describe table extended demo.race2

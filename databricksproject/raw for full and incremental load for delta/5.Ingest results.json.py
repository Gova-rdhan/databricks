# Databricks notebook source
# MAGIC %run "../IncrementalLoad/2.incremental_load_functions"

# COMMAND ----------

dbutils.widgets.text("p_filedate","")
v_date = dbutils.widgets.get("p_filedate")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
from pyspark.sql.functions import current_timestamp,col,lit

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("time",StringType(),True),
    StructField("statusId",IntegerType(),True),
    StructField("resultId",IntegerType(),True),
    StructField("rank",IntegerType(),True),
    StructField("raceId",IntegerType(),True),
    StructField("positionText",StringType(),True),
    StructField("positionOrder",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("points",FloatType(),True),
    StructField("number",IntegerType(),True),
    StructField("constructorId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("fastestLap",IntegerType(),True),
    StructField("fastestLapSpeed",StringType(),True),
    StructField("grid",IntegerType(),True),
    StructField("laps",IntegerType(),True),
    StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

df = spark.read.schema(results_schema).json(f"abfss://rawdb@covidapp3.dfs.core.windows.net/{v_date}/results.json")

# COMMAND ----------

df = df.withColumn("ingestion_date",current_timestamp())\
         .withColumn("file_Date",lit(v_date))

# COMMAND ----------

df = df.dropDuplicates(["raceId","driverId"])

# COMMAND ----------

# METHOD1 USING PARTITION
# for raceid_list in df1.select("raceId").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("processeddb.results"):
#         spark.sql(f"ALTER TABLE processeddb.results DROP IF EXISTS PARTITION (raceId = {raceid_list.raceId})")
#df1.write.mode("append").partitionBy("raceId").format("parquet").saveAsTable("processeddb.results")

# COMMAND ----------

# def rearrange_cols(part_col,df_nam):
#     col_list = []
#     for colname in df_nam.schema.names :
#         if colname != part_col:
#             col_list.append(colname)
#     col_list.append(part_col)
#     final = df_nam.select(col_list)
#     return final
# rt = rearrange_cols('raceId',df)
# print(rt)

# COMMAND ----------

#METHOD2 USING INSERT INTO AND PARTITION BY
# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# if spark._jsparkSession.catalog().tableExists("processeddb.resultss"):
#     rt.write.mode("overwrite").insertInto("processeddb.resultss")
# else:
#     rt.write.mode("overwrite").partitionBy("raceId").format("parquet").saveAsTable("processeddb.resultss")

# COMMAND ----------

# incremental_load(df,'processeddb','resultss','raceId')

# COMMAND ----------

# from delta.tables import DeltaTable
# from pyspark.sql.functions import current_timestamp
# if spark._jsparkSession.catalog().tableExists("deltadb.result"):
#     deltaTablePeople = DeltaTable.forPath(spark, 'abfss://delta@covidapp3.dfs.core.windows.net/result')
#     deltaTablePeople.alias('tgt') \
#     .merge(
#     df.alias('src'),
#     'tgt.resultId = src.resultId'
#   ) \
#   .whenMatchedUpdateAll() \
#   .whenNotMatchedInsertAll()\
#   .execute()
# else:
#     df.write.mode("overwrite").partitionBy("raceId").format("delta").saveAsTable("deltadb.result")

# COMMAND ----------

incremental_upsert('deltadb','result','delta',df,'raceId','tgt.resultId = src.resultId AND tgt.raceId = src.raceId')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltadb.result

# COMMAND ----------

# %sql
# drop table processeddb.resultss

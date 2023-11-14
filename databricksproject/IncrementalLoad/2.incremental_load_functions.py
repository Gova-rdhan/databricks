# Databricks notebook source
def rearrange_cols(part_col,df_nam):
    col_list = []
    for colname in df_nam.schema.names :
        if colname != part_col:
            col_list.append(colname)
    col_list.append(part_col)
    final = df_nam.select(col_list)
    return final

# COMMAND ----------

def incremental_load(op_df,dbname,tbname,part_col):
    op = rearrange_cols(part_col,op_df) 
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if spark._jsparkSession.catalog().tableExists(f"{dbname}.{tbname}"):
        op.write.mode("overwrite").insertInto(f"{dbname}.{tbname}")
    else:
        op.write.mode("overwrite").partitionBy(part_col).format("parquet").saveAsTable(f"{dbname}.{tbname}")

# COMMAND ----------

# def incremental_load(input_df,dbname,tbname,part_col):
#     op_df = rearrange_cols(part_col,input_df)
#    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#    if spark._jsparkSession.catalog().tableExists(f"{dbname}.{tbname}"):
#       op_df.write.mode("overwrite").insertInto(f"{dbname}.{tbname}")
#    else:
#       op_df.write.mode("overwrite").partitionBy(part_col).format("parquet").saveAsTable(f"{dbname}.{tbname}")

# COMMAND ----------

def incremental_upsert(dbname,tbname,cname,df_name,part_col,merge_condition):
    from delta.tables import DeltaTable
    from pyspark.sql.functions import current_timestamp
    if spark._jsparkSession.catalog().tableExists(f"{dbname}.{tbname}"):
        deltaTablePeople = DeltaTable.forPath(spark, f'abfss://{cname}@covidapp3.dfs.core.windows.net/{tbname}')
        deltaTablePeople.alias('tgt') \
        .merge(
        df_name.alias('src'),
        merge_condition
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll()\
    .execute()
    else:
        df_name.write.mode("overwrite").partitionBy(part_col).format("delta").saveAsTable(f"{dbname}.{tbname}")

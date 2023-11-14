# Databricks notebook source
# MAGIC %run "../Processed/1.demo"

# COMMAND ----------

dbutils.widgets.text("data_suore","")
data = dbutils.widgets.get("data_suore")

# COMMAND ----------

client_id = "6307a699-9571-4ada-9abb-6324755dabe3"
tenant_id = "5b61e0d6-f7fb-43b6-ab7f-370d7abaf722"
secret_value = "1zF8Q~KmVT0GHn7T2gRweB3H85sUit6ei6k3cawu"

# COMMAND ----------

# Set OAuth2 configurations
spark.conf.set("fs.azure.account.auth.type.covidapp3.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.covidapp3.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.covidapp3.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.covidapp3.dfs.core.windows.net", secret_value)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.covidapp3.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# List files in the Azure Blob Storage container
display(dbutils.fs.ls("abfss://raw@covidapp3.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@covidapp3.dfs.core.windows.net"))

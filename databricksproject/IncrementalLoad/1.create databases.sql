-- Databricks notebook source
create database if not exists processeddb
location "abfss://processedtb@covidapp3.dfs.core.windows.net/"

-- COMMAND ----------

create database if not exists presentationdb
location "abfss://presentationdb@covidapp3.dfs.core.windows.net/"

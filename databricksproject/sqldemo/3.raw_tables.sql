-- Databricks notebook source
use raw

-- COMMAND ----------

drop table if exists circuits;
create table circuits(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string
)
using csv
options(path "abfss://raw@covidapp3.dfs.core.windows.net/circuits.csv",header True)

-- COMMAND ----------

drop table if exists races;
create table races(
  raceId int,
  year int,
  round int,
  circuitId int,
  name string,
  date string,
  time string,
  url string
)
using csv
options(path "abfss://raw@covidapp3.dfs.core.windows.net/races.csv",header True)

-- COMMAND ----------

drop table if exists results;
create table results(
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grid int,
  position int,
  positionText int,
  positionOrder int,
  points int,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapSpeed int,
  fastestLapTime int,
  statusId int
)
using json
options(path "abfss://raw@covidapp3.dfs.core.windows.net/results.json")

-- COMMAND ----------

drop table if exists constructors;
create table constructors(
  constructorId int,
  constructorRef string,
  name string,
  nationality string,
  url string
)
using json
options(path "abfss://raw@covidapp3.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

drop table if exists drivers;
create table drivers(
  code string,
  dob string,
  driverId int,
  driverRef string,
  name struct<forename string,surname string>,
  nationality string,
  number int,
  url string
)
using json
options(path "abfss://raw@covidapp3.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

drop table if exists lap_times;
create table lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds double
)
using csv
options(path "abfss://raw@covidapp3.dfs.core.windows.net/lap_times")

-- COMMAND ----------

drop table if exists qualifying;
create table qualifying(
  constructorId int,
  driverId int,
  number int,
  position int,
  q1 string,
  q2 string,
  q3 string,
  qualifyId int,
  raceId int
)
using json
options(path "abfss://raw@covidapp3.dfs.core.windows.net/qualifying","multiLine" True)

-- COMMAND ----------

drop table if exists pit_stops;
create table pit_stops(
  driverId int,
  duration string,
  lap int,
  milliseconds string,
  raceId int,
  stop int,
  time string
)

-- COMMAND ----------

select * from pit

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.option("multiline",True).json("abfss://raw@covidapp3.dfs.core.windows.net/pit_stops.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("pit")

-- COMMAND ----------

select * from raw.circuits

-- COMMAND ----------

select * from raw.results

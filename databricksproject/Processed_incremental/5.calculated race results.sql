-- Databricks notebook source
use presentationdtb

-- COMMAND ----------

drop table if exists calculated_results;
create table calculated_results
using parquet
as 
select rc.race_year,rc.race_name,d.name as driver_name,c.constructor_name,r.points,r.position,11-r.position as calculate_points
from processedtb.results r
join processedtb.drivers d on r.driverId = d.driverId
join processedtb.constructors c on r.constructorId = c.constructor_id
join processedtb.races rc on r.raceId = rc.race_id
where r.position < 10

-- COMMAND ----------

SELECT driver_name,count(1) as total_races,sum(calculate_points) as total_points,avg(calculate_points) as avg_points,rank() OVER (ORDER BY avg(calculate_points) DESC) AS RANK 
FROM presentationdtb.calculated_results GROUP BY driver_name HAVING count(1) > 50 ORDER BY RANK ASC;

-- COMMAND ----------

select * from presentationdtb.calculated_results

-- COMMAND ----------

SELECT constructor_name,count(1) as total_races,sum(calculate_points) as total_points,avg(calculate_points) as avg_points,rank() OVER (ORDER BY avg(calculate_points) DESC) as rank 
FROM presentationdtb.calculated_results GROUP BY constructor_name HAVING count(1) > 50 ORDER BY rank ASC;

-- COMMAND ----------



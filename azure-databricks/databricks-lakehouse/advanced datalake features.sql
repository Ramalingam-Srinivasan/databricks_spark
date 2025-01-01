-- Databricks notebook source
describe history employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

select * from employee version as of 1

-- COMMAND ----------

select * from employee@v1

-- COMMAND ----------

select * from employee

-- COMMAND ----------

restore table employee to version as of 1

-- COMMAND ----------

select * from employee

-- COMMAND ----------

describe history employee

-- COMMAND ----------

delete from  employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

restore table employee to version as of 1

-- COMMAND ----------

describe history employee

-- COMMAND ----------

optimize employee zorder by id

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

VACUUM employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

VACUUM employee RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC vacuum command used to clear unused data

-- COMMAND ----------

vacuum employee retain 0 hours

-- COMMAND ----------

select * from employee

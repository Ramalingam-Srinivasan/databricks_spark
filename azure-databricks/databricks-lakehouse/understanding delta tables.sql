-- Databricks notebook source
create table Employee 
(id INT,name STRING,salary DOUBLE)

-- COMMAND ----------

insert into Employee values  
(1, "Adam", 3500.0),
(2, "Sarah", 4020.5),
(3, "John", 2999.3),
(4, "Thomas", 4000.3),
(5, "Anna", 2500.0),
(6, "Kim", 6200.3);

-- COMMAND ----------

select * from Employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### exploring table metadata

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exploring Table Directory

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

update employee set salary = salary + 100 where id = 2

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

-- COMMAND ----------

describe history employee

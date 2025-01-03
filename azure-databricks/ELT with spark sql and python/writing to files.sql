-- Databricks notebook source
-- MAGIC %run /Workspace/Users/rsrinivasan6787@altimetrik.com/databricks_spark/azure-databricks/Includes/Copy-Datasets

-- COMMAND ----------

create table orders as 
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC overwriting tables

-- COMMAND ----------

create or replace table orders as 
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders 
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders
select * , current_timestamp from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

insert into orders 
select * from parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

describe history orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC merging data  

-- COMMAND ----------

create or replace temp view customer_updates as 
select * from json.`${dataset.bookstore}/customers-json-new`

-- COMMAND ----------

merge into customers c
using customer_updates u on 
c.customer_id = u.customer_id
when matched and c.email IS NULL AND u.email IS NOT NULL then
update set c.email = u.email , updated = u.updated
when not matched then insert *

-- COMMAND ----------

create or replace temp view book_updates
(book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
using csv
options (path "${dataset.bookstore}/books-csv-new", header "true",delimeter=";");


-- COMMAND ----------

select count(*) from book_updates;

-- COMMAND ----------

MERGE INTO books b
USING book_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *


-- COMMAND ----------

select count(*) from books

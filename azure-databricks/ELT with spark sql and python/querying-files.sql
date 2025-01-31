-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##querying json

-- COMMAND ----------

-- MAGIC %run /Workspace/Users/rsrinivasan6787@altimetrik.com/databricks_spark/azure-databricks/Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select count(*) from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * , input_file_name() as source_file from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

create table book_csv (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE) 
using csv 
options(header = "true",delimeter=";")
LOCATION '${dataset.bookstore}/books-csv'

-- COMMAND ----------

select * from book_csv

-- COMMAND ----------

describe extended book_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read \
-- MAGIC     .table("book_csv") \
-- MAGIC         .write \
-- MAGIC             .mode('append') \
-- MAGIC                 .format("csv") \
-- MAGIC                 .option('header', 'true') \
-- MAGIC                     .option("delimeter", ";") \
-- MAGIC                         .save(f"{dataset_bookstore}/books-csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

select count(*) from book_csv

-- COMMAND ----------

refresh table book_csv

-- COMMAND ----------

select count(*) from book_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

select count(*) from customers

-- COMMAND ----------

describe extended customers

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

-- COMMAND ----------

select * from books_unparsed

-- COMMAND ----------

create temp view books_temp_view
 (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
 using csv 
 options(
  path "${dataset.bookstore}/books-csv/export_*.csv",
  header "true",
  delimiter ";"
 );

-- COMMAND ----------

select * from books_temp_view

-- COMMAND ----------



-- COMMAND ----------

create table books as select * from books_temp_view

-- COMMAND ----------

select * from books

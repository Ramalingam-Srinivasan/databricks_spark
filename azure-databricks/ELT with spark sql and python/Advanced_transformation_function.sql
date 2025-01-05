-- Databricks notebook source
-- MAGIC %run /Workspace/Users/rsrinivasan6787@altimetrik.com/databricks_spark/azure-databricks/Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Parsing JSON Data

-- COMMAND ----------

select * from customers

-- COMMAND ----------

describe customers

-- COMMAND ----------

describe history customers

-- COMMAND ----------

select customer_id,email,profile:first_name,profile:last_name,profile:address:city from customers limit 5

-- COMMAND ----------

SELECT from_json(profile, 'struct<first_name:string, last_name:string , gender:string ,
address:string>') AS profile_struct FROM customers;

-- COMMAND ----------

SELECT
  profile_struct.first_name,
  profile_struct.last_name,
  profile_struct.gender,
  address_info.street,
  address_info.city,
  address_info.country
FROM (
  SELECT
    from_json(profile, 'struct<first_name:string, last_name:string, gender:string, address:string>') AS profile_struct
  FROM customers
) AS customer_with_profile
LATERAL VIEW 
  from_json(customer_with_profile.profile_struct.address, 'struct<street:string, city:string, country:string>') AS address_info



-- COMMAND ----------

create or replace temp view parsed_customer as 
select customer_id ,  from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;

-- COMMAND ----------

select * from parsed_customer

-- COMMAND ----------

select count(1) from customers

-- COMMAND ----------

select count(1) from parsed_customer

-- COMMAND ----------

describe parsed_customer

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customer

-- COMMAND ----------

create or replace temp view customers_final as 
select customer_id , profile_struct.* from parsed_customer

-- COMMAND ----------

select * from customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders

-- COMMAND ----------

select order_id, customer_id, explode(books) as book from orders order by customer_id

-- COMMAND ----------

select customer_id ,
collect_set(order_id) as order_set,
collect_set(books.book_id) as book_set from orders 
group by customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Flatten Arrays
-- MAGIC

-- COMMAND ----------

select customer_id ,
collect_set(order_id) as order_set,
array_distinct(flatten(collect_set(books.book_id)) ) as flatten_set from orders 
group by customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  Join Operations

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

-- COMMAND ----------

select * from orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Set Operations
-- MAGIC

-- COMMAND ----------

create or replace temp view orders_updates as 
select * from parquet.`${dataset.bookstore}/orders-new`;

-- COMMAND ----------

select count(1) from orders

-- COMMAND ----------

select count(1) from orders_updates

-- COMMAND ----------

select * from orders 
union
select * from orders_updates

-- COMMAND ----------

select * from orders
intersect 
select * from orders_updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reshaping Data with Pivot

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

-- COMMAND ----------

select * from transactions

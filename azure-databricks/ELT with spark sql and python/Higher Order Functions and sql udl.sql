-- Databricks notebook source
-- MAGIC %run /Workspace/Users/rsrinivasan6787@altimetrik.com/databricks_spark/azure-databricks/Includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Filtering Arrays

-- COMMAND ----------

select order_id,
books , filter(books,b -> b.quantity>=2)as multiple_books from orders

-- COMMAND ----------

select order_id , multiple_books from 
(select order_id,
books , filter(books,b -> b.quantity>=2)as multiple_books from orders) where size (multiple_books) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Transforming Arrays

-- COMMAND ----------

select order_id ,
books ,
TRANSFORM  (books,b -> cast (b.subtotal * 3.2 as int))as subtotal_discount from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC User Defined Functions (UDF)

-- COMMAND ----------

create or replace function get_url (email STRING)
returns STRING

return concat('https://www.',split(email,'@')[1]);

-- COMMAND ----------

select email , get_url(email) as domain from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

select  email ,site_type(email) as doamin_type from customers

-- COMMAND ----------

drop function get_url

-- COMMAND ----------

drop function site_type

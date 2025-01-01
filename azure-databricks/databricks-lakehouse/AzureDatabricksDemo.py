# Databricks notebook source
# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

filename = dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

display(filename)

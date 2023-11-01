# Databricks notebook source
dbutils.fs.ls("/FileStore/Users/andrij.demianczuk@databricks.com/tmp")

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
dbutils.fs.ls(f"/Users/{current_user}/")

# COMMAND ----------

# Create the working directory if not exists. If it exists, nothing will happen
dbutils.fs.mkdirs(f"/Users/{current_user}/data")

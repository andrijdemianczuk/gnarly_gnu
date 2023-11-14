# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

catalog = "ademianczuk"
database = "flights"

# COMMAND ----------

lost_bags_df = spark.table(f"{catalog}.{database}.lost_bags_summary")
display(lost_bags_df)

# COMMAND ----------

# DBTITLE 1,Lost bag count by flight_id
lost_bags_flight_id = lost_bags_df.groupBy("flight_id", "destination").count()
lost_bags_flight_id.write.format('delta').option("mergeSchema", True).mode('overwrite').saveAsTable(f"{catalog}.{database}.lost_bags_by_flight")

# Databricks notebook source
# DBTITLE 1,Initialize and load the datasets
from pyspark.sql.functions import *

catalog = "ademianczuk"
database = "flights"

lost_bags_df = spark.table(f"{catalog}.{database}.lost_bags_summary")
display(lost_bags_df)

# COMMAND ----------

# DBTITLE 1,Lost bag count by flight_id
lost_bags_flight_id = lost_bags_df.groupBy("destination").count()
lost_bags_flight_id.write.format('delta').option("mergeSchema", True).mode('overwrite').saveAsTable(f"{catalog}.{database}.lost_bags_by_destination")

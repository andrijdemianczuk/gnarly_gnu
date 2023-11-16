# Databricks notebook source
# DBTITLE 1,Initialize and load the initial datasets
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog = "ademianczuk"
database = "flights"

flights_df = spark.table(f"{catalog}.{database}.flight_schedule")
bags_df = spark.table(f"{catalog}.{database}.bag_tracking")
onboaded_df = spark.table(f"{catalog}.{database}.onboarded_bags_summary")

# COMMAND ----------

# DBTITLE 1,Enrich the datasets post-filter
w = Window.partitionBy("Flight_Number").orderBy(col("Departure_Time").desc())
flights_df = flights_df.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")

flights_df = (flights_df
              .withColumnRenamed("Flight_Number", "flight_id")
              .withColumnRenamed("Destination", "destination")
              .withColumnRenamed("Departure_Time", "departure_time")
              .withColumnRenamed("Cutoff_Time", "cutoff_time"))

flights_df = (flights_df.join(onboaded_df, on="flight_id"))
flights_df.show()

# COMMAND ----------

# DBTITLE 1,Commit the enriched dataset to delta
flights_df.write.format('delta').option("mergeSchema", True).mode('overwrite').saveAsTable(f"{catalog}.{database}.flight_details_summary")

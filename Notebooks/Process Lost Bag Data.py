# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog = "ademianczuk"
database = "flights"

flights_df = spark.table(f"{catalog}.{database}.flight_schedule")
bags_df = spark.table(f"{catalog}.{database}.bag_tracking")

# COMMAND ----------

# DBTITLE 1,Build the latest collection of flights
w = Window.partitionBy("Flight_Number").orderBy(col("Departure_Time").desc())
flights_df = flights_df.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")

flights_df = (flights_df
              .withColumnRenamed("Flight_Number", "flight_id")
              .withColumnRenamed("Destination", "destination")
              .withColumnRenamed("Departure_Time", "departure_time")
              .withColumnRenamed("Cutoff_Time", "cutoff_time"))

# COMMAND ----------

# DBTITLE 1,Sample the data to confirm the structure
display(flights_df.limit(10))
display(bags_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Recode the location where the bag was lost / held
lost_df = bags_df.groupBy(col("bag_id"), col("flight_id"), col("passenger")).count().where(col("count") < 5)
lost_df = (
  lost_df.withColumn("location_lost", when(col("count") == 2, "Security")
                     .when(col("count")==3, "Sorting")
                     .otherwise("Onboarding")))

# COMMAND ----------

# DBTITLE 1,Visualize loss location
display(lost_df.sort(col("count").desc()))

# COMMAND ----------

# DBTITLE 1,Add Departure Times to flights & Write
lost_df = (lost_df.join(flights_df, on="flight_id"))
lost_df.write.format('delta').option("mergeSchema", True).mode('overwrite').saveAsTable(f"{catalog}.{database}.lost_bags_summary")

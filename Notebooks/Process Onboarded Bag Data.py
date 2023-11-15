# Databricks notebook source
# DBTITLE 1,Initialize our imports and dataframes
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog = "ademianczuk"
database = "flights"

flights_df = spark.table(f"{catalog}.{database}.flight_schedule")
bags_df = spark.table(f"{catalog}.{database}.bag_tracking")

# COMMAND ----------

# DBTITLE 1,Preview our dataframes
flights_df.show(5)
bags_df.show(5)

# COMMAND ----------

# DBTITLE 1,Get only the rows showing a bag has been loaded
loaded_df = (bags_df.filter(col("location")=="on plane"))
loaded_df.show(5)

# COMMAND ----------

perc_df = (loaded_df
             .groupBy("flight_id", "passenger_count")
             .agg(sum("bag_weight_kg"), count("bag_id"))
             .withColumnRenamed("sum(bag_weight_kg)", "loaded_bag_weight")
             .withColumnRenamed("count(bag_id)", "bags_loaded")
             .withColumn("perc_bags_loaded", round((col("bags_loaded")/col("passenger_count"))*100,2)))

# display(perc_df.withColumn("perc_onboarded", round((col("count") / col("passenger_count")*100),2)))
display(perc_df)

# COMMAND ----------

perc_df.write.format('delta').option("mergeSchema", True).mode('overwrite').saveAsTable(f"{catalog}.{database}.onboarded_bags_summary")

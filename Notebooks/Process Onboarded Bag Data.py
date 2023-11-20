# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <img class="image" src="https://www.airport-technology.com/wp-content/uploads/sites/14/2017/10/main-584.jpg" width=500 /><br/>
# MAGIC <!-- img src="https://cdn.sanity.io/images/4z7b4ki2/production/52a906bc084b53b971299c06fe0d87be5b7b10a6-1076x555.jpg" width=750 / -->
# MAGIC # Airline Baggage Processing
# MAGIC
# MAGIC Effective baggage handling is essential for ensuring passenger satisfaction, maintaining operational efficiency, ensuring safety and security, managing revenue and costs, building brand reputation and loyalty, complying with regulatory requirements, and integrating with other services. It's a complex operation that plays a vital role in the overall success of an airline.
# MAGIC
# MAGIC Travellers are less incentivized to check baggage due to rising travel costs, time engagement at checkin and collection as well as reliability of bags making their final destination.
# MAGIC
# MAGIC In an effort to improve the traveller experience, baggage telemetry can be utilized to improve visibility and reliability in the checked baggage experience.
# MAGIC
# MAGIC <style>
# MAGIC .image{
# MAGIC   padding: 20px;
# MAGIC   float: right;
# MAGIC }
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Visualizing Data
# MAGIC
# MAGIC Visualizing data is the fastest and easiest way to concisely and accurately evaluate data. Understanding data structure and context is critically important to understanding how we can move forward and use the data points to make business decisions. Here we will visualize our data as a 2-dimensional matrix with the intent of understanding the relationship between loaded bags and bag weight. There are assumptions for example that destinations that are further from our point of origin will have a greater carry weight per-bag. We also want to evaluate what the relationship is with per-capita loaded percentage.

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

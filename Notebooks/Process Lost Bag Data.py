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
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog = "ademianczuk"
database = "flights"

flights_df = spark.table(f"{catalog}.{database}.flight_schedule")
bags_df = spark.table(f"{catalog}.{database}.bag_tracking")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## PySpark Window Functions
# MAGIC <img src="https://miro.medium.com/v2/resize:fit:1400/1*JM3d1-XMixo_szMl4sdHKA.png" width=600 />
# MAGIC
# MAGIC
# MAGIC PySpark window functions are a useful way to evaluate rank of individual elements within a sub-group of results. Since Flight IDs can be re-used over time, we want to ensure that we're only using the most current one. What we are going to do is essentially rank all Flight IDs by timestamp and filter for the top-most id to evaluate our remaining data against. This is an example of a declarative function that can run in parallel rather than an imperative function that runs in serial. This takes advantage of Python on Spark as we can process this data within the context of each shuffle partition for performance and scalability.
# MAGIC
# MAGIC <style>
# MAGIC .image{
# MAGIC   padding: 20px;
# MAGIC   float: left;
# MAGIC }
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px; height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #nf9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px; margin-left: -35px;}
# MAGIC .badge_b { 
# MAGIC   margin-left: 25px; min-height: 32px;}
# MAGIC </style>

# COMMAND ----------

# DBTITLE 1,Build the latest collection of flights
"w = Window.partitionBy("Flight_Number").orderBy(col("Departure_Time").desc())
flights_df = flights_df.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")

flights_df = (flights_df
              .withColumnRenamed("Flight_Number", "flight_id")
              .withColumnRenamed("Destination", "destination")
              .withColumnRenamed("Departure_Time", "departure_time")
              .withColumnRenamed("Cutoff_Time", "cutoff_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Previewing our Dataframes
# MAGIC
# MAGIC We're going to quickly preview our dataframes here to make sure they sync up nicely on `flight_id`. Since we're focused only on groups of events that don't have an `on plane` event we can reliably determine that any group with fewer than five events didn't make it on the aircraft

# COMMAND ----------

# DBTITLE 1,Sample the data to confirm the structure
display(flights_df.limit(10))
display(bags_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizing Lost Bag Data
# MAGIC Sometimes having a visual representation of the residuals and magnitude of data helps us understand where and how we may want to evolve our exploratory data analysis efforts. For example, understanding that lost bags tend to happen in security screening might help us understand why that's the case. This can often lead to extended investigation and use case justification that's validated quantitatively.

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

# MAGIC %md
# MAGIC ## Joining Data
# MAGIC <img src="https://www.rforecology.com/joins_image0.png" width=500/>
# MAGIC
# MAGIC Joining data to create new tables can be an efficient means to evolving structured data. It's important however to do this as late-stage as possible as joining tables is a technically expensive operation. When joining tables, it's also important to understand the downstream implications and applications of the data. Ideally joined tables should be re-used in more than one or two circumstances.

# COMMAND ----------

# DBTITLE 1,Add Departure Times to flights & Write
lost_df = (lost_df.join(flights_df, on="flight_id"))
lost_df.write.format('delta').option("mergeSchema", True).mode('overwrite').saveAsTable(f"{catalog}.{database}.lost_bags_summary")

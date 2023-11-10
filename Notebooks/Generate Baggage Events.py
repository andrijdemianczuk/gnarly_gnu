# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from faker import Faker
from faker_airtravel import AirTravelProvider

import random
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Define our Bag as an Object
from datetime import datetime

class Bag:
  
  def __init__(self):
    pass

  def checkBag(self, flight_id:str="unknown"):
    # print(flight_id)
    checkinTime = datetime.now()


# COMMAND ----------

catalog = "ademianczuk"
database = "flights"

flightsDF = (
    spark.table(f"{catalog}.{database}.flight_schedule")
    .orderBy(col("Departure_Time").desc())
    .limit(4)
)
flight_IDs = (
    flightsDF.select(flightsDF.Flight_Number).rdd.flatMap(lambda x: x).collect()
)

# for each flight, generate the number of passenger and checked bags
for i in flight_IDs:
    passenger_count = random.randint(120, 165)
    bag_count = random.randint(65, 100)
    for _ in range(bag_count):
        bag = Bag()
        bag.checkBag(flight_id=i)

# COMMAND ----------



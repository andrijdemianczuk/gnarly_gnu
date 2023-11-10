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
from faker import Faker
import random


class Bag:
    def __init__(self, windowStart, windowEnd, passenger_count:int = 0):
        self.fake = Faker()
        self.setWeight()
        self.setPassengerName()
        self.setTimeOrigin()
        self.windowStart = windowStart
        self.windowEnd = windowEnd
        self.passengerCount = passenger_count

    def setWeight(self):
        self.weight = random.randint(15, 40)

    def getWeight(self):
        return self.weight
      
    def setPassengerName(self):
      self.passenger_name = self.fake.name_nonbinary()

    def getPassengerName(self):
      return self.passenger_name
    
    def setTimeOrigin(self):
      self.time_origin = self.fake.date_time_between_dates(windowStart, windowEnd)

    def getTimeOrigin(self):
      return self.time_origin

    def checkBag(self, flight_id: str = "unknown"):
        bag_number = self.fake.random_number(digits = 9)
        bag_weight = self.getWeight()
        passenger_name = self.getPassengerName()
        time_origin = self.getTimeOrigin()
        
        print(
            f"bag: {bag_number} flight ID: {flight_id} bag weight: {bag_weight} passenger: {passenger_name} checked in at: {time_origin}. Total passengers: {passenger_count}"
        )

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
    passenger_count = random.randint(100, 165)
    bag_count = random.randint(65, 100)

    then = datetime.now() - timedelta(hours=2)  # use now(tz=timezone.utc) for universal time
    windowStart = datetime.strptime(then.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%M:%S')
    windowEnd = datetime.strptime(then.strftime('%Y-%m-%d %H:29:59'), '%Y-%m-%d %H:%M:%S')

    for _ in range(bag_count):
        bag = Bag(windowStart=windowStart, windowEnd=windowEnd, passenger_count=passenger_count)
        bag.checkBag(flight_id=i)

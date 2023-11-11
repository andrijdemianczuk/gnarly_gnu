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
    def __init__(self, windowStart, windowEnd, flight_id:str="unknown", passenger_count:int = 0):
        self.fake = Faker()
        self.setWeight()
        self.setPassengerName()
        self.setTimeOrigin()
        self.windowStart = windowStart
        self.windowEnd = windowEnd
        self.passengerCount = passenger_count
        self.flight_id = flight_id
        self.isLost = False
        self.conveyor_c = ["C1", "C2", "C3", "C4"]
        self.conveyor_s = ["S1", "S2"]
        self.conveyor_r = ["R1", "R2", "R3"]
        self.conveyor_g = ["G1", "G2", "G3", "G4"]


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

    def checkBag(self):
        bag_number = self.fake.random_number(digits = 9)
        bag_weight = self.getWeight()
        passenger_name = self.getPassengerName()
        time_origin = self.getTimeOrigin()
        
        
        print(
            f"bag: {bag_number} flight ID: {self.flight_id} bag weight: {bag_weight} passenger: {passenger_name} checked in at: {time_origin}. Total passengers: {passenger_count}"
        )

    def secureBag(self):
      offset = random.randint(3,10)
      self.isLost = self.fake.boolean(1)
      
    def routeBag(self):
      offset = random.randint(2,9)
      self.isLost = self.fake.boolean(1)

    def gateBag(self):
      offset = random.randint(4,11)
      self.isLost = self.fake.boolean(1)

    def onboardBag(self):
      self.isLost = self.fake.boolean(1)
      print("onboarding.......")
      

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
        bag = Bag(windowStart=windowStart, windowEnd=windowEnd, flight_id=i, passenger_count=passenger_count)
        
        # This is where the bag gets it's initial attributes
        bag.checkBag()

        # Bag goes through security screening - has a chance of being flagged 
        bag.secureBag()
        
        # Bag gets routher to the gate - has a chance of getting lost here
        bag.routeBag()

        # Bag is gated
        bag.gateBag()

    # At the cutoff time, board the bags on to the plane
    # If bag made it to the gate. Has a chance of being dropped here
    bag.onboardBag()

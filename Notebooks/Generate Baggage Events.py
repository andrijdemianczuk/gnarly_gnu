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
    
    def getIsLost(self):
      return self.isLost

    def checkBag(self) -> list:
      self.bag_number = self.fake.random_number(digits = 9)
      bag_weight = self.getWeight()
      passenger_name = self.getPassengerName()
      time_origin = self.getTimeOrigin()
      conveyor = random.choice(self.conveyor_c)
      
      # print(
      #     f"bag: {self.bag_number} location: {conveyor} flight ID: {self.flight_id} bag weight: {bag_weight} passenger: {passenger_name} checked in at: {time_origin}. Total passengers: {passenger_count}"
      # )
      return {"list"}

    def secureBag(self) -> list:
      offset = random.randint(3,10)
      self.isLost = self.fake.boolean(5) # high likelihood of getting pulled at security
      self.time_s = self.getTimeOrigin() + timedelta(minutes=offset)
      bag_weight = self.getWeight()
      passenger_name = self.getPassengerName()
      conveyor = random.choice(self.conveyor_s)

      # print(
      #   f"bag: {self.bag_number} location: {conveyor} flight ID: {self.flight_id} bag weight: {bag_weight} passenger: {passenger_name} scanned at: {self.time_s}. Total passengers: {passenger_count}"
      # )
      return {"list"}
      
    def routeBag(self) -> list:
      offset = random.randint(2,9)
      self.isLost = self.fake.boolean(1)
      self.time_r = self.time_s + timedelta(minutes=offset)
      bag_weight = self.getWeight()
      passenger_name = self.getPassengerName()
      conveyor = random.choice(self.conveyor_r)

      # print(
      #   f"bag: {self.bag_number} location: {conveyor} flight ID: {self.flight_id} bag weight: {bag_weight} passenger: {passenger_name} scanned at: {self.time_r}. Total passengers: {passenger_count}"
      # )
      # return {"list"}

    def gateBag(self) -> list:
      offset = random.randint(4,11)
      self.isLost = self.fake.boolean(1)
      self.time_g = self.time_r + timedelta(minutes=offset)
      bag_weight = self.getWeight()
      passenger_name = self.getPassengerName()
      conveyor = random.choice(self.conveyor_g)

      # print(
      #   f"bag: {self.bag_number} location: {conveyor} flight ID: {self.flight_id} bag weight: {bag_weight} passenger: {passenger_name} scanned at: {self.time_g}. Total passengers: {passenger_count}"
      # )
      return {"list"}

    def onboardBag(self, cutoff) -> list:
      self.isLost = self.fake.boolean(1)
      bag_weight = self.getWeight()
      passenger_name = self.getPassengerName()

      # print(
      #   f"bag: {self.bag_number} location: on plane flight ID: {self.flight_id} bag weight: {bag_weight} passenger: {passenger_name} scanned at: {cutoff}. Total passengers: {passenger_count}"
      # )
      return {"list"}

# COMMAND ----------

catalog = "ademianczuk"
database = "flights"
index = 0
rows_list=[]

flightsDF = (
    spark.table(f"{catalog}.{database}.flight_schedule")
    .orderBy(col("Departure_Time").desc())
    .limit(4)
)
flight_IDs = (
    flightsDF.select(flightsDF.Flight_Number).rdd.flatMap(lambda x: x).collect()
)

flight_cutoffs = (
    flightsDF.select(flightsDF.Cutoff_Time).rdd.flatMap(lambda x: x).collect()
)

bagsPDF = (
    pd.DataFrame(columns=["bag_id", "location", "flight_id", "bag_weight_kg", "passenger", "time_stamp", "passenger_count"])
)

# for each flight, generate the number of passenger and checked bags
for i in flight_IDs:
    passenger_count = random.randint(100, 165)
    bag_count = random.randint(65, 100)

    then = datetime.now() - timedelta(hours=2)  # use now(tz=timezone.utc) for universal time
    windowStart = datetime.strptime(then.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%M:%S')
    windowEnd = datetime.strptime(then.strftime('%Y-%m-%d %H:29:59'), '%Y-%m-%d %H:%M:%S')
    cutoff_time = flight_cutoffs[index]
    index += 1

    for _ in range(bag_count):
        bag = Bag(windowStart=windowStart, windowEnd=windowEnd, flight_id=i, passenger_count=passenger_count)
        
        # This is where the bag gets it's initial attributes
        bag.checkBag()

        # Bag goes through security screening - has a chance of being flagged 
        if not (bag.getIsLost()):
            bag.secureBag()
        
        # Bag gets routher to the gate - has a chance of getting lost here
        if not (bag.getIsLost()):
            bag.routeBag()

        # Bag is gated
        if not (bag.getIsLost()):
            bag.gateBag()

        # At the cutoff time, board the bags on to the plane
        # If bag made it to the gate. Has a chance of being dropped here
        if not (bag.getIsLost()):
            bag.onboardBag(cutoff_time)

# COMMAND ----------

rows_list = []
for row in input_rows:
    dict1 = {} #list
    # get input row in dictionary format
    # key = col_name
    dict1.update(blah..) 

    rows_list.append(dict1)

df = pd.DataFrame(rows_list)    

# Databricks notebook source
# DBTITLE 1,Imports
from faker import Faker
from faker_airtravel import AirTravelProvider
from datetime import datetime, timedelta

import pandas as pd
import random
import pytz

# COMMAND ----------

# DBTITLE 1,Initialize Variables
fake = Faker()
catalog = "ademianczuk"
database = "flights"

# COMMAND ----------

# DBTITLE 1,Build the Airport IATA Code List
airportsDF = spark.table(f"{catalog}.{database}.canada_iata_codes")
airportsDF = airportsDF.filter(airportsDF.IATA != "YYC")
iata_codes = airportsDF.select(airportsDF.IATA).rdd.flatMap(lambda x: x).collect()
# iata_codes

# COMMAND ----------

# DBTITLE 1,Build the Pandas Dataframe
flightsDF = pd.DataFrame(columns=("Flight_Number", "Destination", "Departure_Time", "Cutoff_Time"))

now = datetime.now()
then = datetime.now() - timedelta(hours=1)  # use now(tz=timezone.utc) for universal time
windowStart = datetime.strptime(then.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%M:%S')
windowEnd = datetime.strptime(then.strftime('%Y-%m-%d %H:59:59'), '%Y-%m-%d %H:%M:%S')
mins = 0

for i in range(4):
  flights = [
    f"WS{random.randint(1111,9999)}",
    random.choice(iata_codes),
    windowEnd - timedelta(minutes=15 + mins),
    windowEnd - timedelta(minutes=30 + mins)
  ]
  flightsDF.loc[i] = [item for item in flights]
  mins += 5

flightsDF

# COMMAND ----------

# DBTITLE 1,Write to Delta
fDF = spark.createDataFrame(flightsDF)
fDF.coalesce(1)
fDF.write.format('delta').option("mergeSchema", True).mode('append').saveAsTable(f"{catalog}.{database}.flight_schedule")

# COMMAND ----------



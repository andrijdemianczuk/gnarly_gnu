import random
from datetime import timedelta

from faker import Faker
from faker_airtravel import AirTravelProvider
from pyspark.sql.types import StructType, StructField, StringType


class Generate:

    def __init__(self, now, windowStart, windowEnd, spark):
        self.now = now
        self.windowStart = windowStart
        self.windowEnd = windowEnd
        self.spark = spark

    def generateFlights(self, catalog="hive_metastore", database="default", mode="overwrite"):
        try:
            print("generating flights...")

            # Build the first faker object and pass in the airtravel provider
            # https://pypi.org/project/faker_airtravel/
            fake = Faker()
            fake.add_provider(AirTravelProvider)

            # Build a list of available destinations
            df = self.spark.table("ademianczuk.flights.canada_iata_codes").select("IATA")
            df = df.filter(df.IATA != "YYC")
            iata_list = list(df.select('IATA').
                             toPandas()['IATA'])

            # Define the structure of the dataframe
            schema = StructType([StructField("Flight_Number", StringType(), True),
                                 StructField("Destination", StringType(), True),
                                 StructField("Departure_Time", StringType(), True),
                                 StructField("Cutoff_Time", StringType(), True)])

            # Create the loop parameters
            mins = 0  # offset minutes
            schedule = []

            # Iterate and build the schedule itinerary
            for _ in range(4):
                flight_num = f"WS1{random.randint(111, 999)}"
                departure_time = self.windowEnd - timedelta(minutes=15 + mins)
                cutoff_time = departure_time - timedelta(minutes=15)
                destination = random.choice(iata_list)
                mins += 5
                flight = (flight_num, destination, departure_time, cutoff_time)
                schedule.append(flight)

            df = self.spark.createDataFrame(data=schedule, schema=schema)
            df.show()

            df.write.format('delta').option("mergeSchema", True).mode(mode).saveAsTable(
                f"{catalog}.{database}.flight_schedule")

        except Exception as e:
            print(e)

    def generateBags(self):
        print("generating bag data...")

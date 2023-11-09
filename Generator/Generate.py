from datetime import date, datetime, timedelta
import random

import faker
from faker import Faker
from faker_airtravel import AirTravelProvider

class Generate:

    def __init__(self, now, windowStart, windowEnd, spark):
        self.now = now
        self.windowStart = windowStart
        self.windowEnd = windowEnd
        self.spark = spark

    def generateFlights(self):
        try:
            print("generating flights...")

            #Build the first faker object and pass in the airtravel provider (https://pypi.org/project/faker_airtravel/)
            fake = Faker()
            fake.add_provider(AirTravelProvider)

            df = self.spark.table("ademianczuk.flights.canada_iata_codes").select("IATA")
            df = df.filter(df.IATA != "YYC")
            iata_list = list(df.select('IATA').
                       toPandas()['IATA'])

            # Generate flights and add them to the output dataframe
            mins = 0 # offset minutes
            for _ in range(4):
                flight_num = f"WS1{random.randint(111, 999)}"
                departure_time = self.windowEnd - timedelta(minutes=30+mins)
                cutoff_time = departure_time - timedelta(minutes=10)
                airport = random.choice(iata_list)
                mins += 5

                print(f"Flight number {flight_num} to {airport} leaves at {departure_time}. Bag loading cutoff is {cutoff_time}")

        except Exception as e:
            print(e)

    def generateBags(self):
        print("generating bag data...")

from datetime import date, datetime
import random


class Generate:

    def __init__(self, now, windowStart, windowEnd):
        self.now = now
        self.windowStart = windowStart
        self.windowEnd = windowEnd

    def generateFlights(self):
        try:
            print("generating flights...")

            count = 4  # number of flights to generate
            i = 0

            # Generate flights and add them to the output dataframe
            while i < count:
                i += 1
                print(f"WS1{random.randint(111, 999)}")
                print(self.windowStart)
                print(self.windowEnd)

            # define the dataframe schema

            # append the flights to the existing table
        except:
            raise "something happened when generating the flight data"

    def generateBags(self):
        print("generating bag data...")

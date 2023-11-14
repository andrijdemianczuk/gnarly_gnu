# Databricks notebook source
# DBTITLE 1,Import Libraries
# Imports
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Initialize the Variables
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
now = current_timestamp()
then = now + expr('INTERVAL -1 HOURS')
catalog = "ademianczuk"
database = "flights"


# COMMAND ----------

# DBTITLE 1,Define the Lookup Table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class Lookups:

    def __int__(self, val: int = 0):
        self.val = val

    def defineStruct(self, schema):
        print(self.val)
        print(schema)

    def generateAirpots(self, current_user, catalog="ademianczuk", database="flights"):
        data = [("YYC","Calgary","AB"),
                ("YEG","Edmonton","AB"),
                ("YFC","Fredericton","NB"),
                ("YQX","Gander","NFL"),
                ("YHZ","Halifax","NS"),
                ("YQM","Moncton","NB"),
                ("YUL","Montreal","QC"),
                ("YOW","Ottawa","ON"),
                ("YQB","Quebec City","QC"),
                ("YYT","Saint Johns","NFL"),
                ("YYZ","Toronto","On"),
                ("YVR","Vancouver","BC"),
                ("YWG","Winnipeg","MB")]

        schema = StructType([ StructField("IATA",StringType(), True),
                              StructField("City",StringType(), True),
                              StructField("Province", StringType(), True)])

        df = spark.createDataFrame(data=data, schema=schema)
        # df.show()

        df.write.format('delta').option("mergeSchema", True).mode('overwrite').saveAsTable(f"{catalog}.{database}.canada_iata_codes")

# COMMAND ----------

# DBTITLE 1,Write the Lookup Table
if not (spark.catalog.tableExists(f"{catalog}.{database}.canada_iata_codes")):
  lk = Lookups()
  lk.generateAirpots(current_user=current_user, catalog=catalog, database=database)

# lk = Lookups()
# lk.generateAirpots(current_user=current_user, catalog=catalog, database=database)

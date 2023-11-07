from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class Lookups:

    def __int__(self,  spark, val: int = 0):
        self.val = val
        self.spark = spark

    def defineStruct(self, schema):
        print(self.val)
        print(schema)

    def generateAirpots(self, spark, current_user, catalog="ademianczuk", database="flights"):
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
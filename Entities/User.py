from pyspark.sql.functions import expr


class User:
    def __init__(self):
        pass

    def getCurrent(spark):
        return (spark
        .createDataFrame([("",)], "user string")
        .withColumn("user", expr("current_user"))
        .head()["user"])

from pyspark.sql.functions import expr


class User:
    def getCurrent(spark):
        return (spark
        .createDataFrame([("",)], "user string")
        .withColumn("user", expr("current_user"))
        .head()["user"])

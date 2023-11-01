from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import *
from Entities.User import User

import Entities.User
from Entities.User import User


# def get_current_user_name():
#     return (spark
#     .createDataFrame([("",)], "user string")
#     .withColumn("user", f.expr("current_user"))
#     .head()["user"])

if __name__ == '__main__':
    spark = DatabricksSession.builder.profile("ml-1").getOrCreate()
    w = WorkspaceClient(profile="ml-1")
    dbutils = w.dbutils

    current_user = User.getCurrent(spark)

    # Make the directory if it doesn't exist. This will be relative for each user who runs this
    dbutils.fs.mkdirs(f"/Users/{current_user}/data")

    for i in (dbutils.fs.ls(f"/Users/{current_user}/data")):
        print(i)

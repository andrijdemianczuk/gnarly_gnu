import faker
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from datetime import datetime, date, time, timezone, timedelta
from databricks.sdk.runtime import *
from Entities.User import User
from faker import Faker
import random
from faker.providers import BaseProvider as fake

import Entities.User
from Entities.User import User

if __name__ == '__main__':
    # spark = DatabricksSession.builder.profile("ml-1").getOrCreate()
    # w = WorkspaceClient(profile="ml-1")
    # dbutils = w.dbutils


    #Initialize values and settings
    # current_user = User.getCurrent(spark)
    now = datetime.now(tz=timezone.utc)
    then = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    windowStart = datetime.strptime(then.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%M:%S')
    windowEnd = datetime.strptime(then.strftime('%Y-%m-%d %H:59:59'), '%Y-%m-%d %H:%M:%S')

    print(now)
    print(windowStart)
    print(windowEnd)

    # Make the directory if it doesn't exist. This will be relative for each user who runs this
    # dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage")

    # df_medunits = spark.table("ademianczuk.hls.l_nc_medunits").limit(5)
    # medunits = df_medunits.select(df_medunits.Short).rdd.flatMap(lambda x: x).collect()
    #reason = ['Sick', 'Family Emergency', 'No-Show', 'Vacation', 'Training', 'PTO']
    # df_medunits.show()

    # df_medunits.write.parquet(path=f"/Users/{current_user}/data/airlines/baggage/test.parquet")

    # for i in (dbutils.fs.ls(f"/Users/{current_user}/data/airlines/baggage")):
    #     print(i)
    # Faker.seed(0)
    # for _ in range(5):
    #     fake.lexify(text='Random Identifier: ??????????')





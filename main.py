# Databricks Imports
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from datetime import datetime, date, time, timezone, timedelta
from databricks.sdk.runtime import *

# Faker / Random Imports
from faker import Faker
import random
from faker.providers import BaseProvider as fake

import Entities.Lookups
# Entity Imports
from Entities.User import User
from Entities.Lookups import Lookups

if __name__ == '__main__':
    spark = DatabricksSession.builder.profile("ml-1").getOrCreate()
    w = WorkspaceClient(profile="ml-1")
    dbutils = w.dbutils

    # Initialize values and settings
    current_user = User.getCurrent(spark=spark)
    now = datetime.now(tz=timezone.utc)
    then = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    windowStart = datetime.strptime(then.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%M:%S')
    windowEnd = datetime.strptime(then.strftime('%Y-%m-%d %H:59:59'), '%Y-%m-%d %H:%M:%S')

    # Make the directories if they don't exist. This will be relative for each user who runs this
    dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage")
    dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage/lookups")
    dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage/flights")
    dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage/bagtracking")

    # Create the lookup tables for airports
    Lookups().generateAirpots(spark=spark, current_user=current_user)

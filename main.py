# Databricks Imports
from datetime import datetime, timedelta

import faker_airtravel
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

# Entity Imports
from Entities.Lookups import Lookups
from Entities.User import User
from Generator.Generate import Generate

from faker import Faker

# Faker / Random Imports

if __name__ == '__main__':
    try:
        # Start - Comment out this block if running from within a databricks workspace ###
        spark = DatabricksSession.builder.profile("ml-1").getOrCreate()
        w = WorkspaceClient(profile="ml-1")
        dbutils = w.dbutils
        # End ############################################################################

        # Initialize values and settings
        current_user = User.getCurrent(spark=spark)
        now = datetime.now()
        then = datetime.now() - timedelta(hours=1)  # use now(tz=timezone.utc) for universal time
        windowStart = datetime.strptime(then.strftime('%Y-%m-%d %H:00:00'), '%Y-%m-%d %H:%M:%S')
        windowEnd = datetime.strptime(then.strftime('%Y-%m-%d %H:59:59'), '%Y-%m-%d %H:%M:%S')

        # Get the config values from the YAML file
        catalog = "ademianczuk"
        database = "flights"

        # Make the directories if they don't exist. This will be relative for each user who runs this
        dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage")
        dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage/lookups")
        dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage/flights")
        dbutils.fs.mkdirs(f"/Users/{current_user}/data/airlines/baggage/bagtracking")

        # Create the lookup tables for airports if it doesn't exist
        if not (spark.catalog.tableExists(f"{catalog}.{database}.canada_iata_codes")):
            Lookups().generateAirpots(spark=spark, current_user=current_user)


        # Create an instance of the generator
        generator = Generate(now=now, windowStart=windowStart, windowEnd=windowEnd, spark=spark)

        # Invoke the flights for the past hour
        generator.generateFlights(mode="overwrite", catalog=catalog, database=database)

        # Invoke the bag history for the past hour
        generator.generateBags()

    except Exception as e:
        print(f"something went wrong...{e}")

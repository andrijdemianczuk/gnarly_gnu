# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
# from databricks.sdk.runtime import *

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = DatabricksSession.builder.profile("ml-1").getOrCreate()
    w = WorkspaceClient(profile="ml-1")

    #Test the workspace client version of dbutils
    dbutils = w.dbutils
    for i in (dbutils.fs.ls("/FileStore/Users/andrij.demianczuk@databricks.com/data")):
        print(i)

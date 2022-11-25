from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

#Get Table Name from spark.catalog.listTables(database) except views

def GetTableName(DatabaseName):
  tables_collection = spark.catalog.listTables(DatabaseName)
  table_names_in_db=[]
  for table in tables_collection:
      if not table.name.startswith('vw_'):   # Avoid the view table
        table_names_in_db.append(table.name )
  return table_names_in_db
hivedb=""
tables_collection=GetTableName(DatabaseName=hivedb)
    #print(tables_collection)

#https://github.com/momataj/azure-databricks/blob/c3facc06054c0a27949b4d87cd6c2323725e89f3/databricks_vacumm.py

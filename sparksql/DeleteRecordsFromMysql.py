from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
Url="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb"
user="myuser"
pwd="mypassword"
con = driver_manager.getConnection(Url, user, pwd)
statement="""
Delete from EMP where sal<=2000"""
exec_statement = con.prepareCall(statement)
exec_statement.execute()

#https://github.com/momataj/azure-databricks/blob/c3facc06054c0a27949b4d87cd6c2323725e89f3/databrick_sqlconnection.py

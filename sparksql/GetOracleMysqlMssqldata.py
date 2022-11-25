from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb"
uname="myuser"
pwd="mypassword"

df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable","emp").option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").load()
df.show()
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
url="jdbc:sqlserver://sateesh.c07uo2a23um4.ap-south-1.rds.amazonaws.com:1433;databaseName=testing"
df=spark.read.format("jdbc").option("url",url).option("user","dbuser").option("password","mspassword").option("dbtable","EMP").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
df.show()
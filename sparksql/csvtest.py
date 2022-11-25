from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
url="jdbc:sqlserver://sateesh.c07uo2a23um4.ap-south-1.rds.amazonaws.com:1433;databaseName=testing"
df=spark.read.format("jdbc").option("url",url).option("user","dbuser")\
    .option("password","mspassword").option("dbtable","banktab")\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
df.show()
df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where age>60 and marital='married'")
res=df.where((col("age")>60) & (col("marital")=="married"))
res.show()
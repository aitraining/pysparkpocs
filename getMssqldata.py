from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
host="jdbc:sqlserver://mssqldb.cudbkjodhypj.ap-south-1.rds.amazonaws.com:1433;databaseName=sqooppoc"

df=spark.read.format("jdbc").option("url",host)\
    .option("password","mspassword")\
    .option("dbtable","(select * from EMP where sal>2500) temp").option("user","msuser")\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
df=df.na.fill(0,["COMM"])
df.show()
#: java.lang.ClassNotFoundException: com.microsoft.sqlserver.jdbc.SQLServerDriver
#if u get ClassNotFoundException its dependency error. u must add appropriate jars/dependencies and place at spark/jars folder
#dbtable: read entire all table records
op="E:\\bigdata\\output\\results"
#df.write.mode("append").format("csv").option("header","true").save(op)

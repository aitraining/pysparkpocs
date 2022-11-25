from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

url="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb"
df = spark.read.format("jdbc").option("url", url).option("dbtable", "emp").option("user","myuser").option("driver","com.mysql.jdbc.Driver").option("password","mypassword").load()
res=df.where(col("sal")>2500)
op="s3://venusparkpoc2022/output/mysql2s3glue"
res.write.format("csv").option("header","true").save(op)
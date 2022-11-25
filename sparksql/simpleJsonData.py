from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\zips.json"
df=spark.read.format("json").load(data)
#df.show(truncate=False)
#df.printSchema()
#ndf=df.withColumnRenamed("_id","id").withColumn("loc", explode(col("loc")))
ndf1=df.withColumnRenamed("_id","id").withColumn("lang", col("loc")[0]).withColumn("lati", col("loc")[1]).drop("loc")
ndf1.createOrReplaceTempView("tab")
ndf=spark.sql("select * from tab where state='CA'")
ndf.show()
ndf.printSchema()
op="E:\\bigdata\\datasets\\output\\resultjson"
ndf.write.mode("append").format("csv").option("header","true").save(op)
#simple datatypes: int, string, double, date etc
#complex datatypes: Array, Struct, Map
# special characters not recommended _id .. rename to id
#explode simple explode /unnest data means remove arrays elements
#url="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb"
#ndf.write.format("jdbc").option("url", url).option("dbtable", "jsontomysql").option("user","myuser").option("driver","com.mysql.jdbc.Driver").option("password","mypassword").save()

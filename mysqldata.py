from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").load(data)
#df.show()
#process
df.createOrReplaceTempView("abcd")
res=spark.sql("select * from abcd where state='CA'")
res.show()
op="E:\\bigdata\\drivers\\output\\result"
#res.write.format("csv").option("header","true").save(op)
host="jdbc:mysql://oct30pocdb.cpnk9yb0uvo8.ap-south-1.rds.amazonaws.com:3306/testing"

res.write.mode("append").format("jdbc").option("url",host).option("user","myuser")\
    .option("password","mypassword").option("dbtable","oct30poc")\
    .option("driver","com.mysql.jdbc.Driver")\
    .save()

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\asldata.txt"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab where Location='chennai'")
#res=spark.sql("select * from tab where Age>=50")
res=spark.sql("select Location, count(*) cnt from tab group by Location order by cnt desc")
res.show()
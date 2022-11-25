from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\task.txt"
df=spark.read.format("csv").option("header","true").load(data)
df.createOrReplaceTempView("tab")

res=spark.sql("select name from tab where subject in ('english','maths') group by name  having count(distinct name) = 2")
#res=df.where((col("subject").isin("english")) & (col("subject").isin("maths")))
#res=df.groupBy(col("name")).agg(collect_list(col("subject")).alias("lst")).withColumn("lst",	array_remove(col("lst")," "))\
#    .where((array_contains(col("lst"),"english")) & (array_contains(col("lst"),"maths")))
res.show()
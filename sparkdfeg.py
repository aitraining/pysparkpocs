from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\donations.csv"
#dataframe creation
df=spark.read.format("csv").option("header","true").load(data)
#df.show()
#header (default false): uses the first line as names of columns.
#res=df.where(col("name")=="venu")
#select  * from tab where name='venu'
#res=df.groupBy("name").agg(sum(col("amount")).alias("tot")).orderBy(col("tot").desc())
df.createOrReplaceTempView("tab")
#createOrReplaceTempView .. used to run sql queries on top of dataframe
#res=spark.sql("select name, sum(amount) amt from tab group by name having sum(amount)>=17000")
res=df.groupBy("name").agg(sum(col("amount")).alias("tot")).where(col("tot")>=17000)
op="E:\\bigdata\\output\\donationsop"
res.coalesce(1).write.format("csv").option("header","true").save(op)
res.show()
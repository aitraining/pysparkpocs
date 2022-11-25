from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\cricket.txt"
df=spark.read.format("csv").option("header","true").load(data)
df.show()
res=df.withColumn("country1", split(col("country"),"-")[0])\
    .withColumn("country2", split(col("country"),"-")[1])\
    .withColumn("score1", split(col("score"),"-")[0])\
    .withColumn("score2", split(col("score"),"-")[1])
res.show()
#res.printSchema()
#fres=res.groupBy(col("country1"),col("country2")).count(col("score1"),col("score2"))
fres=res.withColumn("coun",)
fres.show()


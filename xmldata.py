from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").config('spark.executor.extraClassPath', 'E:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\spark-xml_2.12-0.13.0.jar').appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\xmldata.xml"
#spark.executor.extraClassPath
df=spark.read.format("xml").option("rowTag","course").load(data)
#df.show()
df.createOrReplaceTempView("tab")
res=spark.sql("select *, place.room, place.building, time.start_time, time.end_time from tab").drop("place","time")
#ndf=res.groupBy(col("building")).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
ndf=res.groupBy(col("building"),col("room")).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())
res.show()
res.printSchema()
op="E:\\bigdata\\datasets\\output\\booksxml2csv"
#res.write.format("csv").option("header","true").save(op)
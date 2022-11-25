from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\emailsmay4.txt"
rdd=spark.sparkContext.textFile(data)
res=rdd.filter(lambda x:"@" in x).map(lambda x:x.split(" ")).map(lambda x:(x[0]+x[1], x[-1])).toDF(["name","email"])
res.show(truncate=False)
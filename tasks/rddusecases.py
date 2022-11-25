from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\samp.txt"
rdd = spark.sparkContext.textFile(data)
skip7 = rdd.zipWithIndex().filter(lambda x:x[1]>7).map(lambda x:x[0])\
    .map(lambda x:x.split(",")).toDF(["name","age","city"])

skip7.show()




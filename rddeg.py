from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
#lst=[1,2,3,41,15,6,9,23]
#covnert python elemetns to rdd
#lrdd=sc.parallelize(lst)
#process data
#res=lrdd.map(lambda x:x*x).filter(lambda x:x<100)

#2nd way to create rdd
data="E:\\bigdata\\datasets\\asldata.txt"
#external data convert to rdd
ardd=sc.textFile(data)
#process
#res=ardd.filter(lambda x:"chennai" not in x)
#map or filter or any other transformations apply a logic on entire line not col by col

res=ardd.map(lambda x:x.split(",")).filter(lambda x:'chennai' in x[2])
for i in res.collect():
    print(i)
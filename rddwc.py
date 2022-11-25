from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

data="E:\\bigdata\\datasets\\emaildata.txt"
rdd=spark.sparkContext.textFile(data)
#what it most repeated keyword

pro = rdd.map(lambda x:x.replace("(","")).flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)\
    .sortBy(lambda x:x[1],False)
#map u ll get results in array/list ['Kasi', 'to', 'Everyone):', '07:16:', 'etikala.bi@gmail.com']
#if u use flatMap u ll get results like string (remove array).. Kasi, to , Everyone ...
#flatmap: apply a map and flatter the results

for x in pro.collect():
    print(x)
from pyspark.sql import *
from pyspark.sql.functions import *
import re
from datetime import datetime



spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
name="i am indira from india now in tirupathi"
start_time = datetime.now()
ch = [x for x in name]
rdd = spark.sparkContext.parallelize(ch)
res=rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)\
    .filter(lambda x:x[1]==1)\
    .sortBy(lambda x:x[1],ascending=False)
for x in res.collect():
    print(x)
end_time = datetime.now()

print('Duration of processing: {}'.format(end_time - start_time))

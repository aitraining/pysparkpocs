from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\drivers\\donations.csv"
drdd=spark.sparkContext.textFile(data)
skip=drdd.first()
res=drdd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))\
    .reduceByKey(lambda x,y:x+y)

for i in res.collect():
    print(i)
#in transformations, if data is shuffle its called wide transformation Its decrease performance
#in Transformations, if data doesn't shuffle ,called narrow transformation.
#if u r doing spark-submit ...application..multiple jobs ... multiple jobs convert to multiple stages, multiple stages convert to multiple tasks.
#map: apply a logic on top of each & every element. How many input elements u have same number of output elements u ll get.

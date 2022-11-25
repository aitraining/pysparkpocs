from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\bank-full.csv"
sc=spark.sparkContext

#2 ways to create rdd.. sparkContext (alias sc) used to create rdd.
#sc.parallalize([1,2,3,4,5]) ... python, scala elements convert to rdd
#sc.textFile(data) .... external data convert to rdd use sc.textfile
brdd = sc.textFile(data,200)
#Personal laptop, where I practice
#res = brdd.map(lambda x:x.replace("\"","")).filter(lambda x:"married" in x)
skip = brdd.first()
res = brdd.filter(lambda x:x!=skip).map(lambda x:x.replace("\"","")).map(lambda x:x.split(";"))\
    .filter(lambda x:(int(x[0])>=90)  & (int(x[5])>30000))
for x in res.take(9):
    print(x)

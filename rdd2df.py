from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

data="E:\\bigdata\\datasets\\emaildata.txt"
rdd=spark.sparkContext.textFile(data)
pro = rdd.filter(lambda x:"@" in x).map(lambda x:x.split(" "))\
    .map(lambda x:(x[0],x[-1]))

#in realtime env ... unstructure data convert to structure ...next
#structure data convert to dataframe to process.
df=pro.toDF(["name","email"])
#toDF used to rename all columns and convert rdd to dataframe.
df.show()
#for x in pro.collect():
#    print(x)
df.createOrReplaceTempView("tab")
res=spark.sql("select * from tab where email like '%gmail.com%'")
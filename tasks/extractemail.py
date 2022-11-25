from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\emaildata.txt"
erdd=spark.sparkContext.textFile(data)

res=erdd.filter(lambda x:"@" in x).map(lambda x:x.split(" "))\
    .map(lambda x:(x[0],x[-1])).toDF(["name","email"])
#toDF converts structured rdd to dataframe
#resu=res.where(col("email").like("%gmail%")) # its dataframe
res.show()
#for x in res.collect():
#    print(x)
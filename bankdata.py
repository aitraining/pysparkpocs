from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\bank-full.csv"
#dataframe creation
df=spark.read.format("csv").option("sep",";").option("header","true").load(data)
#df.show()

df.createOrReplaceTempView("tab")
res=spark.sql("select * from tab where age>=60 and balance>=50000")
res.show()
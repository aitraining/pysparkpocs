from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\drivers\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.show()
df.describe("amount").show()
df.groupBy(col("name")).agg(avg("amount"),min("amount"),max("amount"),count("*")).show()

#.describe() function takes cols:String*(columns in df) as optional args.

#.summary() function takes statistics:String*(count,mean,stddev..etc) as optional args.

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\nullsdata.txt"
df=spark.read.format("csv").option("nullValue","null").option("emptyValue"," ").option("header","true").option("inferSchema","true").load(data)
#how many nulls u have in each column
#res=df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])

#fill nulls with 0
#res=df.na.fill(0).na.fill('empty')
#above code u can write like this as well
res=df.na.fill({"email": "no@email", "phone":0})
res.show()
res.printSchema()
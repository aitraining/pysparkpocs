from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\INFI.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#res=df.withColumnRenamed("Adj Close","AdjClose").withColumnRenamed("Volume","vol")
res=df.toDF("Dt","Opn","Hig","Low","Cls","Adj","vol")
win =Window.orderBy(col("Dt"),col("Cls").desc())
res.printSchema()
res=res.withColumn("led", lead(col('Cls'),1,0).over(win))\
    .orderBy(col("Dt").asc())\
    .withColumn("diff",col("Cls")-col("led"))\
    .withColumn("incDec",when(col("diff")>0,"inc").when(col("diff")<0,"dec").otherwise("noChange"))

fres=res.groupBy(col("incDec")).count()
#withColumnRenamed ...rename single column at a time use

fres.show(100)
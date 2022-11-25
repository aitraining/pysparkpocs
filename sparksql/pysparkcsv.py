from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.storagelevel import *
from pyspark import StorageLevel


spark = SparkSession.builder.appName("test").getOrCreate()
#if u want to do anything with Hive u must use enableHiveSupport() within sparkSession like above line

data="s3://venusparkpoc2022/input/bank-full.csv"
op="s3://venusparkpoc2022/output/bankop"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").load(data)
ndf=df.where(col("age")>=60)
ndf.printSchema()
ndf.write.mode("overwrite").format("csv").option("header","true").save(op)
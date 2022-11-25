from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
adf=spark.read.format("org.apache.spark.sql.cassandra").option("table","asl").option("keyspace","cassdb").load()
adf.show()
edf=spark.read.format("org.apache.spark.sql.cassandra").option("table","emp").option("keyspace","cassdb").load()
edf.show()
#join=adf.join(edf,edf.first_name==adf.name,"inner").drop("first_name")
join=adf.join(edf,edf.first_name==adf.name,"leftouterjoin").drop("first_name")
join.show()

from pyspark.sql import *
from pyspark.sql.functions import *

#spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.jars","E:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\spark-snowflake_2.12-2.11.0-spark_3.1.jar").getOrCreate()

sc=spark.sparkContext
spark.sparkContext._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

sfOptions = {
  "sfURL" : "ke78114.snowflakecomputing.com",
  "sfUser" : "sudheer",
  "sfPassword" : "SFpass@123",
  "sfDatabase" : "abc",
  "sfSchema" : "public",
  "sfWarehouse" : "demo_wh"
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from asl") \
  .option("autopushdown", "off") \
  .load()
df.show()
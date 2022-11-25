from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.jars","E:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\spark-snowflake_2.12-2.11.0-spark_3.1").getOrCreate()

sc=spark.sparkContext
sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "e")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "lNjXtm4XM")

sfOptions ={
"sfURL" : "ox54550.ap-south-1.aws.snowflakecomputing.com",
  "sfUser" : "sep102022",
  "sfPassword" : "",
  "sfDatabase" : "venudb",
  "sfSchema" : "public",
  "sfWarehouse" : "SMALL"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
#net\snowflake\spark\snowflake
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from BANKTAB")\
  .option("autopushdown", "off") \
  .load()
#df.show()

data = "E:\\bigdata\\datasets\\10000Records.csv"
df = spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load(data)
import re

cols=[re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
#df.columns ull get everything in the form of list
#cols = ["EmpID","NamePrefix","FirstName","MiddleInitial","LastName","Gender","EMail","FathersName","MothersName","MothersMaidenName","DateofBirth","TimeofBirth","AgeinYrs","WeightinKgs","DateofJoining","QuarterofJoining","HalfofJoining","YearofJoining","MonthofJoining","MonthNameofJoining","ShortMonth","DayofJoining","DOWofJoining","ShortDOW","AgeinCompanyYears","Salary","LastHike","SSN","PhoneNo","PlaceName","County","City","State","Zip","Region","UserName","Password"]
ndf = df.toDF(*cols)
ndf.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("dbtable",  "tab10ktabnov7")\
  .option("autopushdown", "off") \
  .save()
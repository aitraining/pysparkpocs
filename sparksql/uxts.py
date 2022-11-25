from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "IST")
data="C:\\data\\archive\\CSV\\CSV\\ethereum_dataset.csv"
df=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)
df=df.withColumnRenamed("Date(UTC)","Dateutc").withColumn("Dateutc",to_date(col("Dateutc"),"M/d/yyyy"))\
    .withColumn("cutx",unix_timestamp())\
    .withColumn("UnixTimeStamp1",from_unixtime(col("UnixTimeStamp")))\
    .withColumn("UnixTimeStamp1",to_date(col("UnixTimeStamp1")))\
    .withColumn("UnixTimeStampUST",from_utc_timestamp(col("UnixTimeStamp").cast(TimestampType()),"UTC"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("diff",col("today")-col("UnixTimeStamp1"))\
    .withColumn("dtdiff",datediff(col("today"),col("UnixTimeStamp1")))\
    .withColumn("dtTrunc",date_trunc("second",col("ts")))

df.show()
#londontime (unixtimestamp) convert to UTC TS time.

#.withColumn("UnixTimeStamp1",from_unixtime(col("UnixTimeStamp")))
#its convert uxts to normaldate.
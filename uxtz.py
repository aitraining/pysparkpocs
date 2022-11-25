from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\drivers\\output\\CSV\\CSV\\ethereum_dataset.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
def sec2dt(sec):
    mn = int(sec/60)
    hr = int(sec/3600)
    days = int(hr/24)
    yrs = days // 365
    mns = (days - yrs * 365) // 30
    dys = (days - yrs * 365 - mns * 30)
    hrs = int(dys/24)
    min = int(hrs/60)
    sc= min/60
    result = yrs, " years ", mns, " months ", dys, " days ", hrs, " hours ",min," minutes"
    st = ''.join(map(str, result))
    return st
#python function convert to udf
udfunc = udf(sec2dt)

res=df.select("Date(UTC)","UnixTimeStamp").withColumn("iuxt",unix_timestamp())\
    .withColumn("utx2ist",from_utc_timestamp(from_unixtime(col("UnixTimeStamp")),"IST"))\
    .withColumn("isttime",current_timestamp())\
    .withColumn("diff",col("isttime").cast("long")-col("utx2ist").cast("long"))\
    .withColumn("minus",col("isttime")-col("utx2ist"))\
    .withColumn("task",udfunc(col("diff")))

#5 years, 4 months,2 days , 5 hours, 20 minutes 10 seconds

#how many seconds completed ... diff
#2015-07-30 05:30:00
res.show(truncate=False)
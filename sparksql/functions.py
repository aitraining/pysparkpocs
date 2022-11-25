from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "IST")
data1="E:\\bigdata\\datasets\\uk-trafic\\archive\\EthereumDailyGasUsedHistory.csv"
df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data1)
data="E:\\bigdata\\drivers\\donations.csv"
#dataframe api, rdd api, dstream api, dataset api
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
ndf=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtformat",date_format(col("dt"),"yyyy-MMMM-dd-EEEE-z"))\
    .withColumn("after100days",date_add(col("today"),100))\
    .withColumn("before100days",date_add(col("today"),-100))\
    .withColumn("lastday",last_day(col("dt")))\
    .withColumn("nextday",next_day(col("today"),"Mon"))\
    .withColumn("lastfri",next_day(date_add(last_day("dt"),-7),"Fri"))\
    .withColumn("addmon", add_months(col("today"),100))\
    .withColumn("monbetw", months_between(col("today"),col("dt"),True))\
    .withColumn("round", round(col("monbetw").cast(IntegerType())))\
    .withColumn("ceil", ceil(col("monbetw")))\
    .withColumn("floor", floor(col("monbetw")))\
    .withColumn("dttrunc",date_trunc("minute",col("ts")))\
    .withColumn("qtr", quarter(col("dt")))\
    .withColumn("yr",year(col("today")))\
    .withColumn("mon",month(col("today")))\
    .withColumn("dyofweek",dayofweek(col("today")))\
    .withColumn("dayofyr",dayofyear(col("today")))\
    .withColumn("dayofmon",dayofmonth(col("today")))\
    .withColumn("weekofyr",weekofyear(col("today")))\
    .withColumn("uxts",unix_timestamp())

#from 1970-jan-1-london-time ..today how many seconds completed
#dayofyear... how many days completed from jan 1 ... 286days
#dayofmonth... how many days completed from mon 1 .. oct, 13 so ull get 13
#dayofweek... how many days completed from sun ... sun1, mon2...sat 7
#round : .. if anynum below 0.5 ... ull get floor val means if u have 2.49 ull get 2
#if round ..2.52 its round to 3, but 2.49 u ll get 2
#if 2.99 or 2.01 value floor value u ll get 2 only
#if 2.99 or 2.01 .. ceil value u ll get 3 only.
#datediff.. diff between two dates u ll get in days .. 40 days, 990 days ..
#by default spark able to understand only yyyy-MM-dd format only
#if input data hve dd-mm-yyyy at that time u must use format option
#date_format used to get ur required date format.

ndf.show(truncate=False)
ndf.printSchema()
ndf1=df1.withColumnRenamed("Date(UTC)","DateUTC")\
    .withColumn("DateUTC",to_date(col("dateUTC"),"M/d/yyyy"))\
    .withColumn("UnixTimeStampTime",from_unixtime(col("UnixTimeStamp")))\
    .withColumn("UnixTimeStampIST",from_utc_timestamp(col("UnixTimeStampTime"),"IST"))\
    .withColumn("today",current_date())
ndf1.show(9)
ndf1.printSchema()
res=ndf.join(ndf1,"today","inner").select(ndf.today,ndf1.DateUTC,ndf1.UnixTimeStampIST,ndf.ts)\
    .withColumn("diff",col("ts")-col("UnixTimeStampIST"))\
    .withColumn("dtdiff",col("today")-col("DateUTC"))

res.show(truncate=False)
#withColumnRenamed used to rename old column to new column
host="jdbc:sqlserver://mssqldb.cudbkjodhypj.ap-south-1.rds.amazonaws.com:1433;databaseName=sqooppoc"

ndf.write.format("jdbc").option("url",host).option("password","mspassword").option("dbtable","oct16tab").option("user","msuser").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").save()

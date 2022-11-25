from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\drivers\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#spark by default able to understand 'yyyy-MM-dd' format only
#but in original data u hve dd-MM-yyyy so this date format convert to spark understandable format.
#to_date convert input date format to 'yyyy-MM-dd' format
#current_dat() used to get today date based on ur system time.
#config("spark.sql.session.timeZone", "EST") ... its very imp based on original client date all default time based on us time only.at that time mention "EST"
#current_timestamp() u ll get seconds minutes as well.

#create udf to get expected date format. like 1yr, 2 months, 4 days ..


#.withColumn("daystoyrmon", udffunc(col("dtdiff")))
res=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("today"),100))\
    .withColumn("dtminus",col("today")-col("dt"))\
    .withColumn("dtsub", date_sub(col("today"),100))\
    .withColumn("lastdate",last_day(col("dt")))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMM/yyyy/EEE/Q/z"))\
    .withColumn("dttrunc",date_trunc("minute",col("ts")))\
    .withColumn("nextday",next_day(col("today"),"Mon"))\
    .withColumn("lstfri",next_day(date_sub(last_day(col("dt")),7),"Fri"))\
    .withColumn("task",date_format(col("lastdate"),"EEEE"))\
    .withColumn("dyofyr",dayofyear(col("dt")))\
    .withColumn("dyofmon",dayofmonth(col("dt")))\
    .withColumn("dyofweek",dayofweek(col("dt")))\
    .withColumn("weekofyr",weekofyear(col("dt")))\
    .withColumn("ux",unix_timestamp())
#from jan 1 1970 london time .. today date how many seconds completed
#dayofweek ..sun1..mon2 tue 3 ..wed4 thu5,  fri 6 sat 7


res.show(truncate=False)
res.printSchema()
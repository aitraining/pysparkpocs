from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
data="E:\\bigdata\\drivers\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#spark by default able to understand 'yyyy-MM-dd' format only
#but in original data u hve dd-MM-yyyy so this date format convert to spark understandable format.
#to_date convert input date format to 'yyyy-MM-dd' format
#current_dat() used to get today date based on ur system time.
#config("spark.sql.session.timeZone", "EST") ... its very imp based on original client date all default time based on us time only.at that time mention "EST"
#current_timestamp() u ll get seconds minutes as well.

#create udf to get expected date format. like 1yr, 2 months, 4 days ..
def daystoyrmndays(nums):
    yrs = int(nums / 365)
    mon = int((nums % 365) / 30)
    days = int((nums % 365) % 30)
    result = yrs, "years" , mon , "months" , days, "days"
    st = ''.join(map(str, result))
    return st

udffunc = udf(daystoyrmndays)
#.withColumn("daystoyrmon", udffunc(col("dtdiff")))
res=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("dt"),100))\
    .withColumn("dtsub",date_sub(col("dt"),100))\
    .withColumn("lastdt",date_format(last_day(col("dt")),"yyyy-MM-dd-EEE"))\
    .withColumn("nxtday",next_day(col("dt"),"Friday"))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMMM/yy/EEEE/zzz"))\
    .withColumn("monLstFri",next_day(date_add(last_day(col("dt")),-7),"Fri"))\
    .withColumn("dayofweek", dayofweek(col("dt")))\
    .withColumn("dayofmon",dayofmonth(col("dt")))\
    .withColumn("dayofyr", dayofyear(col("dt")))\
    .withColumn("monbet" ,months_between(current_date(),col("dt")))\
    .withColumn("floor",floor(col("monbet")))\
    .withColumn("ceil",ceil(col("monbet")))\
    .withColumn("round",round(col("monbet")).cast(IntegerType()))\
    .withColumn("dttrunc",date_trunc("day",col("dt")).cast(DateType()))\
    .withColumn("weekofyear",weekofyear(col("dt")))\
    .withColumn("daystoyrmon", udffunc(col("dtdiff")))

res.printSchema()
res.show(truncate=False)
#dtdiff .. 588 days .. i want to conver to 1yr-3mon-4days ..
#dayofweek ... from sun how many days completed.. if sun ..1, mon.2.tue..3..sat 7
#dayofmon .... from month 1 to how many days completed
#dayofyear .... from jan 1 to specified date, how many days completed.
#date_add(df.dt, -100) and date_sub(df.dt, 100) both are same.
#last_day ... it return month's last day.. let jan lastday jan 31.. feb lastday 28
#whats next sun, next mon, next wednesday from today ull get. next_day(dt, "sun")
#https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
#date_format used to get ur desired format date. let eg: 20/April/21/Tuesday/ at that time use     .withColumn("dtformat",date_format(col("dt"),"dd/MMMM/yy/EEEE/"))

#tasks: i want udf get dtdiff conver to 3 years, 4 months 9 days
#every month 15th what day u ll get? (sun?or mon)
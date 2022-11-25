from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "CST")

data="E:\\bigdata\\drivers\\donations.csv"
df=spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)
#processing.
df.withColumn("dt",to_date(col("dt"),"d-M-yyyy")).createOrReplaceTempView("tab")
#res=spark.sql("select name,sum(amount) smamt from test group by name ")
#res=df.groupBy(col("name")).agg(sum(col("amount")).alias("sm"))
res=spark.sql("select *, current_date() today, current_timestamp() ts, datediff(current_date(), dt) dtdiff, date_add(dt,100) dtadd, date_sub(dt, 100) dtsub, last_day(dt) lstday from tab")
res.show()

'''res=df.withColumn("age",lit(18))\
    .withColumn("amount",col("amount")+1000)\
    .withColumn("name",when(col("name")=="anu","anuradha")
                .when(col("name")=="venkat",reverse(col("name")))
                .when((col("name")=="venu")& (col("amount")>=6000),upper(col("name"))).otherwise(col("name")))\
    .withColumn("dt",regexp_replace(col("dt"),"-","/"))\
    .withColumn("dt",to_date(col("dt"),"d/M/yyyy"))\
    .withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtformat",date_format(col("dt"),"yyyy-LLLL-dd-EEEE-D"))\
    .withColumn("dtadd",date_add(col("dt"),10))\
    .withColumn("dtsub",date_add(col("dt"),-11))\
    .withColumn("dtsub1",date_sub(col("dt"),11))\
    .withColumn("lstdate",last_day(col("dt")))\
    .withColumn("admon",add_months(col("dt"),100))\
    .withColumn("datediff", datediff(current_date(),col("dt")))\
    .withColumn("monbet",months_between(current_date(),col("dt")))\
    .withColumn("floor",floor(col("monbet")).cast(IntegerType()))\
    .withColumn("ceil",ceil(col("monbet")).cast(IntegerType()))\
    .withColumn("rnd",round(col("monbet")).cast(IntegerType()))\
    .withColumn("nxtd",next_day(col("dt"),"Sunday"))
'''


#next_day ... in this week whats next week we ll get let eg:
#next sun, next fri .. wats day u ll get

#how many days between dt col and today ... number of days

#date_format used to get date as per our required format
#let eg: 2021-Jan-10-Sun
#https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
#res.printSchema()

#spark by default understand yyyy-MM-dd format only thats y convert input date format to spark understandable format
#use to_date(col("dt"),"d/M/yyyy") at that time input data convert to yyyy-MM-dd
#withColumn used to add new column (if column not exists)
#withColumn used to update existing column (if column exists)

res.show(truncate=False)
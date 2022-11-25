from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "EST")

data="E:\\bigdata\\datasets\\donations.csv"
df=spark.read.format("csv").option('header',"true").option("dateFormat","yyyy-MM-dd").option("inferSchema","true").load(data)
df=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))

'''    
    '''
#10-1-2021
#withcolumn .. used if specified column doesn't exists add new column
#lit function used to add dummy value (anything) string, int, ..
#by default spark consider as all columns are strings. if u mention inferSchema auto data convert to appropriate datatypes. means venu like string, 8888 like int ...
#by default spark understand data like yyyy-MM-dd ... only consider as date otherwise its consider as string


#df.show(25,truncate=5)
df.createOrReplaceTempView("tab")
#createOrReplaceTempView used to run sql queries on top of dataframe
#res=spark.sql("select *, current_date() today , current_timestamp() ts from tab ")
res=df.withColumn("today",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("dtdiff", datediff(col("today"),col("dt")))\
    .withColumn("180dtback",date_add(col("dt"),-180))\
    .withColumn("next180",date_add(col("dt"),180))\
    .withColumn("lastdate", last_day(col("dt")))\
    .withColumn("dtformat",date_format(col("dt"),"yy/LLL/dd/EEEE"))\
    .withColumn("qrt",quarter(col("dt")))\
    .withColumn("nxtday",next_day(col("dt"),"Fri"))\
    .withColumn("saldt",next_day(date_add(last_day(col("dt")),-7),"Sat"))


qry = """
with temp as (select *, current_date() today, date_format(dt,'yyyy/MMM/dd/EEEE') dtformat, next_day(dt,'Fri') nxtday from tab)
select *, datediff(today, dt) dtdiff from temp
"""
#res=spark.sql(qry)
res.show(truncate=False)
res.printSchema()
#printSchema ... used to display columns information in nice tree format
#datediff ... diff between two dates u ll get in the form of days

#show display top 20 rows by default in table format
#in columns value/characters must be below 20. if more than 20 characters it ends with ....
#if u want to display full name, all characers use false


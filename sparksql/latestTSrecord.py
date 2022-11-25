from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\donations.csv"
df=spark.read.format("csv").option("header","true").load(data)

df=df.withColumn("dt",to_date(col("dt"),'d-M-yyyy'))
win=Window.partitionBy("name").orderBy(col("dt").desc())
#res=df.withColumn("latest",row_number().over(win)).where(col("latest")==1).drop("latest")
#yyyy-MM-dd
df.createOrReplaceTempView("tab")
qry="""with temp as (select *, row_number() over (partition by name order by dt desc) as lst from tab)
select * from temp where lst=1 """
res=spark.sql(qry).drop("lst")
res.show()
res.printSchema()
"""
+------+----------+------+
|  name|        dt|amount|
+------+----------+------+
|venkat|2021-10-10|  8000|
|  venu|2021-11-18|  9000|
|  sita|2021-06-10|  5000|
|   anu|2021-12-12|  7000|
+------+----------+------+

+------+----------+------+
|  name|        dt|amount|
+------+----------+------+
|venkat|2021-10-10|  8000|
|  venu|2021-11-18|  9000|
|  sita|2021-06-10|  5000|
|   anu|2021-12-12|  7000|
+------+----------+------+"""
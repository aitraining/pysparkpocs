from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\empmysql.csv"
df=spark.read.format("csv").option("header","true").load(data)
cols=[re.sub('[^a-zA-Z]',"",c) for c in df.columns]
df=df.toDF(*cols).select([re.sub(' ',"",c) for c in df.columns])\
    .withColumn("sal", col("sal").cast(IntegerType()))
df.printSchema()
df.createOrReplaceTempView("tab")
#res=spark.sql("select job, sal from tab")
#pyspark dataframe api
#win=Window.partitionBy("deptno").orderBy(col("sal").desc())
#win=Window.partitionBy().orderBy(col("sal").desc())
#res=df.withColumn("drank",dense_rank().over(win)).where(col("drank")==2)
#res.show()
qry = """with temp as (select *, DENSE_RANK() over (partition by deptno order by sal desc) as drank from tab)
select * from temp where drank=2"""
qry1 = """with temp as (select *, DENSE_RANK() over (partition by '' order by sal desc) as drank from tab)
select * from temp where drank=2"""

res=spark.sql(qry1)
res.show()

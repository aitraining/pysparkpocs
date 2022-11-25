from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\empmysql.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df=df.toDF("empno","ename","job", "mgr", "hiredate", "sal", "comm", "deptno")
#toDF used for rename all columns ... 2nd ... convert rdd to dataframe
import re
cols=[re.sub('[^a-zA-Z0-1]',"",c.lower()) for c in df.columns]
df=df.toDF(*cols)

df.createOrReplaceTempView("tab")
res=spark.sql("select *, lead(sal) over (partition by job order by sal desc) led , lag(sal) over (partition by job order by sal desc) lg from tab")
res=res.withColumn("diff",col("sal")-col("led")).na.fill(0,"diff")
res.show()

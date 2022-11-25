from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\empdata.txt"
#dataframe creation
df=spark.read.format("csv").option("sep",",").option("header","true").load(data)
#df.show()
df.createOrReplaceTempView("t1")
qry="""with t2 as (select  deptid, max(sal) maxsal from t1 group by deptid)
select t1.empid, t1.deptid, t1.sal, t2.maxsal from t1 join t2 on t1.deptid==t2.deptid where sal==maxsal"""
#res=spark.sql(qry).drop("maxsal")
win=Window.partitionBy(col("deptid")).orderBy(col("sal").desc())
res=df.withColumn("rnk",first(col("sal")).over(win)).where(col("sal")==col("rnk")).drop("rnk","doj")
res.show()
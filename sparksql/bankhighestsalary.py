from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data = "E:\\bigdata\\datasets\\bank-salary.txt"
df = spark.read.format("csv").option("header","true").option("sep",",").option("inferschema","true").load(data)
df.createOrReplaceTempView("tab")
res=spark.sql("select a.empid, b.employee, b.salary, b.managerid from tab a join tab b on a.empid==b.managerid where b.salary>a.salary")
res.withColumn("test",col("employee").cast(StringType)).na.drop("")
res.show()
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f


spark = SparkSession.builder.master("local[2]").config("spark.sql.legacy.timeParserPolicy","LEGACY").appName("test").getOrCreate()
sc = spark.sparkContext._active_spark_context

data="E:\\bigdata\\datasets\\dateusecases.txt"
df=spark.read.format("csv").option("header","true").load(data)
#df.show()
#df.printSchema()
# yyyy-MM-dd format only
#data cleaning steps
def dynamic_date(col,frmts=("yyyy-MM-dd","dd-MMM-yyyy","ddMMMMyyyy","MM-dd-yyyy","MMM/yyyy/dd")):
    return coalesce(*[to_date(col, i )for i in frmts])

import re
cols=[re.sub('[^a-zA-Z0-9]','',c) for c in df.columns]
ndf=df.toDF(*cols)
res=ndf.withColumn("birthdob",dynamic_date(col("birthdob")))
res.printSchema()
#res.show()
#data process
res.createOrReplaceTempView("tab")
result = spark.sql("select * from tab where birthdob>'2022-03-23'")
result.show()

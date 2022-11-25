from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\books.xml"
df=spark.read.format("xml").option("rowTag","book").option("path",data).load()
#df.show()
res=df.withColumnRenamed("_id","id").where(col("price")>10)
res.show()
op="E:\\bigdata\\datasets\\output\\xml2csv.csv"
#res.write.mode("overwrite").format("csv").option("header","true").save(op)
res.toPandas().to_csv(op)
res.write.bucketBy()
#Caused by: java.lang.ClassNotFoundException: xml.DefaultSource
#its dependency error download dependency from https://mvnrepository.com/artifact/com.databricks/spark-xml_2.12/0.15.0 place in spark/jars folder
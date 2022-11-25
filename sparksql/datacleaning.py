from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\drivers\\bank-full.csv"
#dataframe api, rdd api, dstream api, dataset api
df=spark.read.format("csv").option("header","true").option("sep",";").load(data)
#df.show()
#data cleaning
#withColumn used to add new column (if column not exists)
#withColumn used to update column (if column already exists)
#lit is a function used to add a dummy value.
res=df.withColumn("gender",lit("M"))\
    .withColumn("housing",when(col("housing")=="yes","S").otherwise("N"))\
    .withColumn("full",concat_ws(" ",col("job"),col("age"),col("marital"),col("education")))\
    .withColumn("balance", lpad(substring(col("balance"),0,2),10,"*"))
#all=int(df.count())
res.show(all,truncate=False)
#show displays top 20 columns and
# if any column contains more than 20 chars its truncate to display


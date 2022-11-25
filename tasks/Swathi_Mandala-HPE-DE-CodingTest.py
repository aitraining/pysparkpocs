from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
import numpy as np
spark=SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\macked_test_data.json"
df=spark.read.format("json").option("multiLine","true").load(data)
#if any spaces available , remove all trims in original data.
df=df.select(*(trim(col(c)).alias(c) for c in df.columns))
#in ranking columns data in string its better split ranks and convert to array<int> datatype
df=df.withColumn("rankings", split(col("rankings"),","))\
    .withColumn("rankings", col("rankings").cast("array<int>"))
df.show()
df.printSchema()
print("task 1 getting little confusing... a person (name) participate multiple times that is in the form of rankings. so participate based on number of ranks.")
#task1=df.groupBy(col("name")).agg(count(col("*")).alias("cnt")).orderBy(col("cnt").desc())
task1=df.withColumn("maxsize",size(col("rankings"))).orderBy(col("maxsize").desc())

task1.show()

print("task 2: List of participants who won Rank 1 most of the times.")
#usually ranking dense_rank highly recommended. but in this scenario rankings not required.
task2=df.withColumn("rankings", explode(col("rankings")))\
    .where(col("rankings")==1).groupBy(col("name"))\
    .agg(count("rankings").alias("cnt")).orderBy(col("cnt").desc())
task2.show()
print("task3: The country with maximum participants.")
#usually to fulfil requirement just group by enough
task3=df.groupBy(col("alphanumeric")).agg(count("*").alias("cnt"))\
    .orderBy(col("cnt").desc())
task3.show()
df.createOrReplaceTempView("tab")
#based on document atleast one task must write in sql.
task3=spark.sql("select alphanumeric, count(*) cnt from tab group by alphanumeric order by cnt desc")
task3.show()
print("task4: The country whose participants had highest average ranking.")
#usually array_max, array_min this type function available, but there is no array_avg or array_mean function. So create udf to fulfill requirement
#array_mean is udf this udf get avg of array elements based on country.
array_mean = udf(lambda x: float(np.mean(x)), FloatType())
task4=df.withColumn("avgarr",array_mean(col("rankings")).alias("avgrank"))\
    .groupBy(col("alphanumeric")).agg(avg("avgarr").alias("cnt")).orderBy(col("cnt").desc())
task4.show()
print(" task5: List of participants who participated the least number of times.")
#This task same like above first task.
#task5=df.groupBy(col("name")).agg(count("name").alias("cnt")).orderBy(col("cnt").asc())
task5=df.withColumn("minsize",size(col("rankings")))\
    .orderBy(col("minsize").asc())

task5.show()
'''
1.	List of participants who has participated the maximum number of times.
2.	List of participants who won Rank 1 most of the times.
3.	The country with maximum participants.
4.	The country whose participants had highest average ranking.
5.	List of participants who participated the least number of times.
'''
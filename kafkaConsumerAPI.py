from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
#data extraction ... get data from source.
df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "nov").load()
#df.printSchema()
df1=df.selectExpr("CAST(value AS STRING)")

#data cleaning & processing
df2 = df1.select(split(df("value"), " ").alias("lg"))

df2 = df1.select(col("lg").getItem(1).alias("ip"), col("lg").getItem(4).alias("date"),
                 col("lg").getItem(6).alias("req"),col("lg").getItem(10).alias("ref"))

df2.printSchema()
#alternative approach
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "(["]*)'

# Extracting the date we are looking for
res = df1.select(regexp_extract('value', log_reg, 1).alias('ip'),
                        regexp_extract('value', log_reg, 4).alias('date'),
                        regexp_extract('value', log_reg, 6).alias('request'),
                        regexp_extract('value', log_reg, 10).alias('referrer'))
res.printSchema()

#store data in oracle (Load)

#res.writeStream.format("console").outputMode("append").start().awaitTermination()
def foreach_batch_function(df, epoch_id):
    ndf=df.withColumn("date", to_date(col("date"),"dd/MMM/yyyy:hh:mm:ss"))

    ndf.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlserver://sateesh.c07uo2a23um4.ap-south-1.rds.amazonaws.com:1433;databaseName=testing") \
        .option("dbtable", "livedatanov23") \
        .option("user", "dbuser") \
        .option("password", "mspassword") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append") \
        .save()

    pass

res.writeStream.foreachBatch(foreach_batch_function)\
    .outputMode("append").start().awaitTermination()
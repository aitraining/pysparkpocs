from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

df=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "log111") \
  .load()

#df.printSchema()
userSchema = StructType().add("name", "string").add("age", "integer")

res=df.selectExpr("CAST(value AS STRING)")
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

logs_df = res.select(regexp_extract('value', log_reg, 1).alias('ip'),
                         regexp_extract('value', log_reg, 4).alias('date'),
                         regexp_extract('value', log_reg, 6).alias('request'),
                         regexp_extract('value', log_reg, 10).alias('referrer'))
#logs_df.show()

#res.printSchema()
fres = logs_df.writeStream.format("console")\
  .trigger(continuous='5 second')\
  .start()
fres.awaitTermination()

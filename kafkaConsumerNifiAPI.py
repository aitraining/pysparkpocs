from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
#data extraction ... get data from source.
df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "novnifi").load()
#df.printSchema()
df1=df.selectExpr("CAST(value AS STRING)")
sch = spark.read.format("json").option("multiLine","true").load("E:\\bigdata\\nifi-1.18.0\\testlogs").schema
res=df1.select(from_json(col("value").cast("string"), sch).alias("parsed_value")).select(col("parsed_value.*"))
def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    cols = [re.sub('[^a-zA-Z0-9_]', "", c.lower()) for c in df.columns]
    df = df.toDF(*cols)
    return df


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df)
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True
    return df;

ndf=flatten(res).na.fill(0)

def foreach_batch_function(df, epoch_id):
    df.write.format("jdbc")\
        .option("url", "jdbc:sqlserver://sateesh.c07uo2a23um4.ap-south-1.rds.amazonaws.com:1433;databaseName=testing")\
        .option("dbtable", "nifilogs2mssql") \
        .option("user", "dbuser") \
        .option("password", "mspassword") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append").save()

    pass


ndf.printSchema()
#ndf.writeStream.format("console").outputMode("append").start().awaitTermination()
ndf.writeStream.foreachBatch(foreach_batch_function)\
    .outputMode("append").start().awaitTermination()
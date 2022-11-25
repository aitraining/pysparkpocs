from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\companies.json"
df=spark.read.format("json").option("multiLine","true").load(data)

def read_nested_json(df):
    column_list = []

    for column_name in df.schema.names:
        print("Outside isinstance loop: " + column_name)
        # Checking column type is ArrayType
        if isinstance(df.schema[column_name].dataType, ArrayType):
            print("Inside isinstance loop of ArrayType: " + column_name)
            df = df.withColumn(column_name, explode(column_name).alias(column_name))
            column_list.append(column_name)

        elif isinstance(df.schema[column_name].dataType, StructType):
            print("Inside isinstance loop of StructType: " + column_name)
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)

    # Selecting columns using column_list from dataframe: df
    df = df.select(column_list)
    cols = [re.sub('[^a-zA-Z0-1]', "", c.lower()) for c in df.columns]
    return df.toDF(*cols)


#ndf = df.toDF(*cols)


#df1=spark.read.format("json").option("multiLine","true").load(data)
#res=read_nested_json(read_nested_json(df1))
#df=res
#df=read_nested_json(read_nested_json(res))
read_nested_json_flag = True

while read_nested_json_flag:
  print("Reading Nested JSON File untill normailization ")
  df = read_nested_json(df=df)
#  df.printSchema()
  read_nested_json_flag = False
  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True

      #df.printSchema()
df.printSchema()
#ndf.printSchema()
#https://github.com/momataj/azure-databricks/blob/c3facc06054c0a27949b4d87cd6c2323725e89f3/nexted_json.py
#https://github.com/dafauti/voyager/blob/6114aacac21c6a9f6fab64322a6fda0cdf6d6fb6/src/main/python/spark_read_json.py

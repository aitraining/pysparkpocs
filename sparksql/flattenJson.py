from pyspark.sql import *
from pyspark.sql.functions import *
import re

from typing import Final, Dict, Tuple
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame as SDF
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, ArrayType

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
path="E:\\bigdata\\datasets\\output\\"
df=spark.read.format("json").option("multiLine","true").load(path)
#df=df.repartition(1000)

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
    return df;


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df);
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True;
    cols = [re.sub('[^a-zA-Z0-1]', "", c.lower()) for c in df.columns]
    return df.toDF(*cols);


res=read_nested_json(df)
res.printSchema()
res.show(4)
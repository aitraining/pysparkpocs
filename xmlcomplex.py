from pyspark.sql import *
from pyspark.sql.functions import *
import re
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\transactions.xml"
df=spark.read.format("xml").option("rowTag","POSLog").load(data)
df.show()
df.printSchema()

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
    cols = [re.sub('[^a-zA-Z0-9_]', "", c) for c in df.columns]
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

ndf=flatten(df)
ndf.printSchema()
result=ndf.groupBy(col("Transaction_CurrencyCode")).count()
result.show()
op="E:\\bigdata\\datasets\\output\\xml2csvcomplex.csv"
#ndf.write.format("csv").option("header","true").save(op)
ndf.toPandas().to_csv(op)

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\testdata.txt"
df = spark.read.format("csv").option("sep",",").option("delim_whitespace",True).option("mode","PERMISSIVE")\
    .option("columnNameOfCorruptRecord","wrong").option("header","true").option("inferSchema","true").load(data)
sch=spark.read.format("csv").option("header","true")\
    .option("inferSchema","true").load(data).schema
newmal=sch.add(StructField("wrong",StringType(),True))
df = spark.read.format("csv").schema(newmal).option("mode","PERMISSIVE")\
    .option("columnNameOfCorruptRecord","wrong").option("header","true").option("inferSchema","true").load(data)
df.show()
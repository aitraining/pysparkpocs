from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="E:\\bigdata\\datasets\\testdata.txt"
sch=spark.read.format("csv").option("header","true")\
    .option("inferSchema","true").load(data).schema
newmal=sch.add(StructField("wrong",StringType(),True))
df = spark.read.format("csv").schema(newmal).option("mode","PERMISSIVE")\
    .option("columnNameOfCorruptRecord","wrong").option("header","true").option("inferSchema","true").load(data)
df.cache()
df.count()
# separate malrecords
#ndf=df.select(col("wrong")).filter(col("wrong").isNotNull())
#get good records
ndf=df.filter(col("wrong").isNull()).drop("wrong")

ndf.show()
#ndf.write.format("csv").save("E:\\bigdata\\datasets\\output\\malrecords1")
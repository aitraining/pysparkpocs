from pyspark.sql import *
import re
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.master("local[3]").appName("test").getOrCreate()
ssc=StreamingContext(spark.sparkContext,10)
path="E:\\bigdata\\datasets\\output\\"
dstream = ssc.textFileStream(path)
#sch=spark.read.format("json").option("multiLine","true").load(path).schema
dstream.pprint()
# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        #rdd1 = rdd.map(lambda x: rdd.loads(x))
        df = spark.read.json(rdd, multiLine=True)
        df.show()
        df.printSchema()
          # Get the singleton instance of SparkSession
        # Convert RDD[String] to RDD[Row] to DataFrame

    except:
        pass


#dstream.foreachRDD(process)
dstream.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
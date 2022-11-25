from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *

spark = SparkSession.builder.master("local[3]").appName("test").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)
# Create a DStream that will connect to hostname:port, like localhost:9999
#SocketTextStream used to get data from Terminal/command prompt from **** server **** port number.
lines = ssc.socketTextStream("ec2-3-111-58-95.ap-south-1.compute.amazonaws.com", 1234)
lines.pprint()

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
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        cols=["name","age","city"]
        df = rdd.map(lambda w:w.split(",")).toDF(cols);
        df.printSchema()
        host = "jdbc:mysql://mysqldb.coaugp4ng2j6.ap-south-1.rds.amazonaws.com:3306/mydbisaac"
        df.write.mode("append").format("jdbc") \
            .option("url", host) \
            .option("user", "myuser") .option("password", "mypassword") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("dbtable", "livedata123").save()
    except:
        pass

lines.foreachRDD(process)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
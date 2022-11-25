from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)
spark.sparkContext.setLogLevel("ERROR")
#spark internally use diff contexts to create different api
#sparkContext ... to crate rdd api
#sqlContext used to create dataframe api
#sparkSession used to create dataset api
#sparkSTreamingContext ..ssc..to create Dstream api
# create dstream.. its very headache process.
#socketTextStream get data from console/terminal from something server & port num
host="ec2-13-233-214-222.ap-south-1.compute.amazonaws.com"
lines = ssc.socketTextStream(host, 1234)
#lines.pprint()
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
        rowRdd = rdd.map(lambda w:w.split(","))

        df=rowRdd.toDF(["name","age","city"])
        df.show()
        hyddf=df.where(col("city")=="hyd")
        host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false"
        hyddf.write.mode("append").format("jdbc").option("url",host).option("user","myuser")\
        .option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver")\
        .option("dbtable","livehydsparkstreaming").save()

    except:
        pass

lines.foreachRDD(process)

ssc.start()             # Start the computation
ssc.awaitTermination()

#ConnectException: Connection timed out: connect
#if u get above error its security group issue. 1234 add 0.0.0.0/0
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)
spark.sparkContext.setLogLevel("ERROR")
data="C:\\Users\\DELL\\Downloads\\docs\\logs\\access*.log"
dstream = ssc.textFileStream(data)
dstream.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination()
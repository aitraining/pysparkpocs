from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation","true")\
        .getOrCreate()


    raw_df = spark.readStream \
        .format("csv").option("sep"," ") \
        .option("path", "C:\\Users\\DELL\\logs\\weblogs\\") \
        .option("maxFilesPerTrigger", 1) \
        .load()


    # this code streaming data make as micro batch data.. that micro batch data save in oracle.. thats y
    # create a function this function used in writesterram to export to mysql

    def foreach_batch_function(df, epoch_id):


        res1=df.withColumnRenamed("_c0","ip").withColumnRenamed("_c3","dtIST").withColumnRenamed("_c5","getorput").withColumnRenamed("_c8","referurl")
        res=res1.select(col("ip"),col("dtIST"),col("getorput"),col("referurl"))

        # Send the dataframe into MongoDB which will create a BSON document out of it

        #min 15%
        res.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false") \
            .option("dbtable", "weblogs11Aug1") \
            .option("user", "myuser") \
            .option("password", "mypassword") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()

        pass


    raw_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start() \
        .awaitTermination()

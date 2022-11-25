from pyspark.sql import *
from pyspark.sql.functions import *
from time import sleep
from kafka import KafkaProducer
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data="C:\\Users\\DELL\\logs\\access_log_20221123-073820.log"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

with open (data,mode="r") as f:
    for line in f:
        print(line)
        producer.send('nov', value=line.encode("ascii"))
        sleep(2)

#key, value
#nov, "107.229.123.77 - - [23/Nov/2022:07:42:10 +0530] "DELETE /apps/cart.jsp?appID=2361 HTTP/1.0" 200 4968 "http://stevens-stanley.com/search/about/" "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_7_5) AppleWebKit/5340 (KHTML, like Gecko) Chrome/13.0.882.0 Safari/5340"


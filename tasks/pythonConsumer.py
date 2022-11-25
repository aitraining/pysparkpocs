import pandas as pd

from pyspark.sql import *
from pyspark.sql.functions import *
from kafka import KafkaConsumer
from json import loads
#spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

consumer = KafkaConsumer(
    'aaa',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
import json
for message in consumer:
    msg = message.value
    dt = json.load(msg)
    df=pd.read_json(msg)
    print(df)

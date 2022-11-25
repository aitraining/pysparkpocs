from pyspark.sql import *
from pyspark.sql.functions import *
import json
from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas as pd
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         json.dumps(x).encode('utf-8'))
import os.path
import os
from pathlib import Path
dat="E:\\bigdata\\datasets\\output"
#dat="E:\\bigdata\\kafka_2.12-2.4.0\\input"
#for e in range(10000):
'''for pat in Path(dat).glob("*.json"):
    with open(pat,"r") as f:
        data=json.load(f)
        df=pd.read_json(data)
        data=df
        producer.send("aaa",value=data)
        print(data)
        sleep(5)
'''
for pat in Path(dat).glob("*"):
    with open(pat,"r") as f:
        lines = f.read()
        print(type(lines))
        producer.send("aaa",value=lines)
        print(lines)
        sleep(5)

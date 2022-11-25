from kafka import KafkaProducer
import os
from time import sleep
from json import dumps

source_dir = 'C:\\data\\access_log_20221102-225002.log'
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open(source_dir, mode='r') as f:
    for line in f:
        print(line)
        producer.send('log111', value=line.encode("ascii"))
        sleep(5)